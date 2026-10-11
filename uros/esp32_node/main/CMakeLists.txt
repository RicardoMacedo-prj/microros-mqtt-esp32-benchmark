idf_component_register(SRCS main.c  
                    INCLUDE_DIRS ""
                    )

target_include_directories(${COMPONENT_LIB} PRIVATE "/home/dev/esp/micro_ros_espidf_component/micro_ros_src/install/include/uros_custom_msgs")