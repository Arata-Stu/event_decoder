#include <rclcpp/rclcpp.hpp>
#include <rosbag2_cpp/reader.hpp>
#include <rosbag2_storage/storage_options.hpp>
#include <rosbag2_cpp/readers/sequential_reader.hpp>
#include <event_camera_msgs/msg/event_packet.hpp>
#include <event_camera_codecs/decoder.h>
#include <event_camera_codecs/decoder_factory.h>
#include <mutex>
#include <queue>
#include <vector>
#include <memory>
#include <string>
#include <H5Cpp.h>
#include <chrono>

// タイミング計測用
using Clock = std::chrono::high_resolution_clock;

#define TIMER_START(label) auto label##_start = Clock::now();
#define TIMER_END(label, message) \
    RCLCPP_INFO(this->get_logger(), "%s took %ld ms", message, std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - label##_start).count());
class ProcessNode : public rclcpp::Node {
public:
    ProcessNode(const rclcpp::NodeOptions &options)
        : Node("bag_event_processor", options), decoder_factory_() 
    {
        // パラメータの設定
        this->declare_parameter<std::string>("bag_file", "path/to/your_bag");
        this->declare_parameter<std::string>("topic_name", "/your_event_topic");
        this->declare_parameter<std::string>("h5_file", "output.h5");
        this->declare_parameter<bool>("debug", false);
        this->declare_parameter<int64_t>("batch_size", static_cast<int64_t>(1000));


        this->get_parameter("bag_file", bag_file_);
        this->get_parameter("topic_name", topic_name_);
        this->get_parameter("h5_file", h5_file_);
        this->get_parameter("debug", debug_);
        int64_t batch_size_int64;
        this->get_parameter("batch_size", batch_size_int64);
        batch_size_ = static_cast<size_t>(batch_size_int64);

        // HDF5ファイルの初期化
        h5file_ = std::make_unique<H5::H5File>(h5_file_, H5F_ACC_TRUNC);

        // バッグファイルの読み込み
        openBag();

        // バッファの処理スレッドを開始
        buffer_processor_thread_ = std::thread([this]() { this->processBuffer(); });
    }

    ~ProcessNode() {
        stop_processing_ = true;
        if (event_reader_thread_.joinable()) {
            event_reader_thread_.join();
        }
        if (buffer_processor_thread_.joinable()) {
            buffer_processor_thread_.join();
        }

        if (debug_) {
            RCLCPP_INFO(this->get_logger(), "Closing H5 file.");
        }
        try {
            h5file_->close();
        } catch (const H5::Exception &err) {
            RCLCPP_ERROR(this->get_logger(), "Error closing HDF5 file: %s", err.getDetailMsg().c_str());
        }
    }

private:
    class MyProcessor : public event_camera_codecs::EventProcessor {
    public:
        explicit MyProcessor(std::queue<std::tuple<uint64_t, uint16_t, uint16_t, uint8_t>> &buffer, std::mutex &mutex)
            : buffer_(buffer), mutex_(mutex) {}

        void eventCD(uint64_t timestamp, uint16_t x, uint16_t y, uint8_t polarity) override {
            std::lock_guard<std::mutex> lock(mutex_);
            buffer_.emplace(timestamp, x, y, polarity);
        }

        void eventExtTrigger(uint64_t, uint8_t, uint8_t) override {}
        void finished() override {}
        void rawData(const char *, size_t) override {}

    private:
        std::queue<std::tuple<uint64_t, uint16_t, uint16_t, uint8_t>> &buffer_;
        std::mutex &mutex_;
    };

    void openBag() {
        rosbag2_storage::StorageOptions storage_options;
        storage_options.uri = bag_file_;
        storage_options.storage_id = "sqlite3";

        rosbag2_cpp::ConverterOptions converter_options{ "cdr", "cdr" };
        reader_ = std::make_unique<rosbag2_cpp::Reader>();
        reader_->open(storage_options, converter_options);

        if (debug_) {
            RCLCPP_INFO(this->get_logger(), "Bag file opened: %s", bag_file_.c_str());
        }

        event_reader_thread_ = std::thread([this]() { this->readEventsFromBag(); });
    }

    void readEventsFromBag() {
        MyProcessor processor(event_buffer_, buffer_mutex_);

        while (reader_->has_next()) {
            TIMER_START(bag_read);
            auto bag_msg = reader_->read_next();
            TIMER_END(bag_read, "Reading a message from bag file");

            if (bag_msg->topic_name == topic_name_) {
                auto event_msg = std::make_shared<event_camera_msgs::msg::EventPacket>();
                rclcpp::SerializedMessage serialized_msg(*bag_msg->serialized_data);
                rclcpp::Serialization<event_camera_msgs::msg::EventPacket> serializer;

                serializer.deserialize_message(&serialized_msg, event_msg.get());

                auto decoder = decoder_factory_.getInstance(*event_msg);
                if (!decoder) {
                    RCLCPP_WARN(this->get_logger(), "Invalid encoding in event message.");
                    continue;
                }

                TIMER_START(decode);
                decoder->decode(*event_msg, &processor);
                TIMER_END(decode, "Decoding an event message");
            }
        }

        stop_processing_ = true;
    }

    void processBuffer() {
        std::vector<std::tuple<uint64_t, uint16_t, uint16_t, uint8_t>> batch;

        while (!stop_processing_ || !event_buffer_.empty()) {
            {
                std::unique_lock<std::mutex> lock(buffer_mutex_);
                while (!event_buffer_.empty() && batch.size() < batch_size_) {
                    batch.push_back(event_buffer_.front());
                    event_buffer_.pop();
                }
            }

            if (!batch.empty()) {
                TIMER_START(h5_batch_write);
                saveBatchToHDF5(batch);
                TIMER_END(h5_batch_write, "Writing batch to HDF5");
                batch.clear();
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }

            if (debug_) {
                RCLCPP_INFO(this->get_logger(), "Buffer size: %zu", event_buffer_.size());
            }
        }
    }

    void saveBatchToHDF5(const std::vector<std::tuple<uint64_t, uint16_t, uint16_t, uint8_t>> &batch) {
        try {
            if (!dataset_initialized_) {
                createHDF5Dataset();
            }

            hsize_t current_size[2] = {current_row_ + batch.size(), 4};
            dataset_->extend(current_size);

            hsize_t offset[2] = {current_row_, 0};
            hsize_t count[2] = {batch.size(), 4};
            H5::DataSpace filespace = dataset_->getSpace();
            filespace.selectHyperslab(H5S_SELECT_SET, count, offset);

            hsize_t dim[2] = {batch.size(), 4};
            H5::DataSpace memspace(2, dim);

            std::vector<int64_t> flat_data;
            for (const auto &event : batch) {
                flat_data.push_back(std::get<0>(event) / 1000);
                flat_data.push_back(std::get<1>(event));
                flat_data.push_back(std::get<2>(event));
                flat_data.push_back(static_cast<int64_t>(std::get<3>(event)));
            }

            dataset_->write(flat_data.data(), H5::PredType::NATIVE_INT64, memspace, filespace);

            current_row_ += batch.size();
        } catch (const H5::Exception &err) {
            RCLCPP_ERROR(this->get_logger(), "Failed to save data to HDF5: %s", err.getDetailMsg().c_str());
        }
    }

    void createHDF5Dataset() {
        hsize_t init_dims[2] = {0, 4};
        hsize_t max_dims[2] = {H5S_UNLIMITED, 4};
        H5::DataSpace dataspace(2, init_dims, max_dims);

        hsize_t chunk_dims[2] = {1024, 4};
        H5::DSetCreatPropList prop;
        prop.setChunk(2, chunk_dims);
        prop.setDeflate(5);

        dataset_ = std::make_unique<H5::DataSet>(
            h5file_->createDataSet("events", H5::PredType::NATIVE_INT64, dataspace, prop));

        dataset_initialized_ = true;
        current_row_ = 0;
    }

    std::string bag_file_;
    std::string topic_name_;
    std::string h5_file_;
    bool debug_;
    size_t batch_size_;

    std::unique_ptr<rosbag2_cpp::Reader> reader_;
    std::queue<std::tuple<uint64_t, uint16_t, uint16_t, uint8_t>> event_buffer_;
    std::mutex buffer_mutex_;

    std::unique_ptr<H5::H5File> h5file_;
    std::thread event_reader_thread_;
    std::thread buffer_processor_thread_;
    bool stop_processing_ = false;

    std::unique_ptr<H5::DataSet> dataset_;
    bool dataset_initialized_ = false;
    hsize_t current_row_ = 0;

    event_camera_codecs::DecoderFactory<event_camera_msgs::msg::EventPacket, MyProcessor> decoder_factory_;
};

int main(int argc, char *argv[]) {
    rclcpp::init(argc, argv);

    auto options = rclcpp::NodeOptions();
    rclcpp::spin(std::make_shared<ProcessNode>(options));

    rclcpp::shutdown();
    return 0;
}