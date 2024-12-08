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
#include <atomic>  // 追加

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
        stop_processing_.store(true);  // atomic に設定
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
            // TIMER_START(bag_read);  // タイマー削除
            auto bag_msg = reader_->read_next();
            // TIMER_END(bag_read, "Reading a message from bag file");  // タイマー削除

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

                // TIMER_START(decode);  // タイマー削除
                decoder->decode(*event_msg, &processor);
                // TIMER_END(decode, "Decoding an event message");  // タイマー削除
            }
        }

        stop_processing_.store(true);  // atomic に設定
    }

    void processBuffer() {
        std::vector<std::tuple<uint64_t, uint16_t, uint16_t, uint8_t>> batch;

        while (!stop_processing_.load() || !event_buffer_.empty()) {
            {
                std::unique_lock<std::mutex> lock(buffer_mutex_);
                while (!event_buffer_.empty() && batch.size() < batch_size_) {
                    batch.push_back(event_buffer_.front());
                    event_buffer_.pop();
                }
            }

            if (!batch.empty()) {
                // TIMER_START(h5_batch_write);  // タイマー削除
                saveBatchToHDF5(batch);
                // TIMER_END(h5_batch_write, "Writing batch to HDF5");  // タイマー削除
                batch.clear();
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }

            if (debug_) {
                RCLCPP_INFO(this->get_logger(), "Buffer size: %zu", event_buffer_.size());
            }
        }

        // バッファ処理が完了した後にノードをシャットダウン
        if (debug_) {
            RCLCPP_INFO(this->get_logger(), "Buffer processing completed. Shutting down node.");
        }
        rclcpp::shutdown();
    }

    void saveBatchToHDF5(const std::vector<std::tuple<uint64_t, uint16_t, uint16_t, uint8_t>> &batch) {
        try {
            if (!dataset_initialized_) {
                createHDF5Dataset();
            }

            hsize_t current_size[1] = {current_row_ + batch.size()};
            timestamp_dataset_->extend(current_size);
            x_dataset_->extend(current_size);
            y_dataset_->extend(current_size);
            polarity_dataset_->extend(current_size);

            hsize_t offset[1] = {current_row_};
            hsize_t count[1] = {batch.size()};
            H5::DataSpace filespace = timestamp_dataset_->getSpace();
            filespace.selectHyperslab(H5S_SELECT_SET, count, offset);

            hsize_t dim[1] = {batch.size()};
            H5::DataSpace memspace(1, dim);

            // 各データ型に対応する配列
            std::vector<int64_t> timestamps;  // int64_t型に変更
            std::vector<uint16_t> x_coords;
            std::vector<uint16_t> y_coords;
            std::vector<uint8_t> polarities;

            timestamps.reserve(batch.size());
            x_coords.reserve(batch.size());
            y_coords.reserve(batch.size());
            polarities.reserve(batch.size());

            for (const auto &event : batch) {
                timestamps.push_back(static_cast<int64_t>(std::get<0>(event)) / 1000);  // ミリ秒に変換
                x_coords.push_back(std::get<1>(event));
                y_coords.push_back(std::get<2>(event));
                polarities.push_back(std::get<3>(event));
            }

            // 各データを個別に保存
            timestamp_dataset_->write(timestamps.data(), H5::PredType::NATIVE_INT64, memspace, filespace);
            x_dataset_->write(x_coords.data(), H5::PredType::NATIVE_UINT16, memspace, filespace);
            y_dataset_->write(y_coords.data(), H5::PredType::NATIVE_UINT16, memspace, filespace);
            polarity_dataset_->write(polarities.data(), H5::PredType::NATIVE_UINT8, memspace, filespace);

            current_row_ += batch.size();
        } catch (const H5::Exception &err) {
            RCLCPP_ERROR(this->get_logger(), "Failed to save data to HDF5: %s", err.getDetailMsg().c_str());
        }
    }

    void createHDF5Dataset() {
        // データセットの初期サイズを設定
        hsize_t init_dims[1] = {0};  // 行数は最初0
        hsize_t max_dims[1] = {H5S_UNLIMITED};  // 行数は無限
        H5::DataSpace dataspace(1, init_dims, max_dims);

        hsize_t chunk_dims[1] = {1024};  // チャンクサイズ（バッチ単位で書き込む）
        H5::DSetCreatPropList prop;
        prop.setChunk(1, chunk_dims);
        prop.setDeflate(5);  // 圧縮レベル

        // 各データ（timestamp, x, y, polarity）用のデータセットを個別に作成
        timestamp_dataset_ = std::make_unique<H5::DataSet>(
            h5file_->createDataSet("t", H5::PredType::NATIVE_INT64, dataspace, prop));
        x_dataset_ = std::make_unique<H5::DataSet>(
            h5file_->createDataSet("x", H5::PredType::NATIVE_UINT16, dataspace, prop));
        y_dataset_ = std::make_unique<H5::DataSet>(
            h5file_->createDataSet("y", H5::PredType::NATIVE_UINT16, dataspace, prop));
        polarity_dataset_ = std::make_unique<H5::DataSet>(
            h5file_->createDataSet("p", H5::PredType::NATIVE_UINT8, dataspace, prop));

        dataset_initialized_ = true;
        current_row_ = 0;
    }

    std::unique_ptr<H5::DataSet> timestamp_dataset_;
    std::unique_ptr<H5::DataSet> x_dataset_;
    std::unique_ptr<H5::DataSet> y_dataset_;
    std::unique_ptr<H5::DataSet> polarity_dataset_;

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
    std::atomic<bool> stop_processing_{false};  // 修正

    bool dataset_initialized_ = false;
    hsize_t current_row_ = 0;

    event_camera_codecs::DecoderFactory<event_camera_msgs::msg::EventPacket, MyProcessor> decoder_factory_;
};

int main(int argc, char *argv[]) {
    rclcpp::init(argc, argv);

    auto options = rclcpp::NodeOptions();
    auto node = std::make_shared<ProcessNode>(options);
    rclcpp::spin(node);

    rclcpp::shutdown();
    return 0;
}
