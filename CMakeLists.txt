cmake_minimum_required(VERSION 3.8)
project(event_decoder)

# コンパイラオプションの設定
if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# 必要なパッケージを検索
find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(rosbag2_cpp REQUIRED)
find_package(event_camera_msgs REQUIRED)
find_package(event_camera_codecs REQUIRED)
find_package(HDF5 REQUIRED COMPONENTS C CXX) # HDF5の追加

# ヘッダーファイルとライブラリのインクルード
include_directories(${HDF5_INCLUDE_DIRS})

# 実行可能ファイルの作成
add_executable(event_decoder src/event_decoder.cpp)
ament_target_dependencies(event_decoder 
  rclcpp 
  rosbag2_cpp 
  event_camera_msgs 
  event_camera_codecs
)
target_link_libraries(event_decoder ${HDF5_LIBRARIES}) # HDF5ライブラリのリンク

# インストールターゲットの指定
install(TARGETS
  event_decoder
  DESTINATION lib/${PROJECT_NAME}
)

install(
  DIRECTORY launch/
  DESTINATION share/${PROJECT_NAME}/launch
)

# amentパッケージとしての設定
ament_package()
