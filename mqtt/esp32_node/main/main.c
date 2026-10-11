#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sdkconfig.h>
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>
#include <freertos/event_groups.h>

#include <esp_timer.h>
#include <esp_log.h>
#include <esp_mac.h>
#include <esp_wifi.h>
#include <esp_event.h>
#include <nvs_flash.h>
#include <mqtt_client.h>

// Wi-Fi Credentials
#define WIFI_SSID "YOUR_WIFI_SSID"
#define WIFI_PASS "YOUR_WIFI_PASSWORD"
#define WIFI_CONNECTED_BIT BIT0

// MQTT Broker URI
#define MQTT_BROKER_URI "mqtt://YOUR_HOST_IP:1883"

#define MAX_SAMPLES 12000
#define SAMPLES_PER_BLOCK 100

// MQTT Binary Payloads Memory Alignment
#pragma pack(push, 1)
typedef struct {
    char device_id[16];
    int64_t timestamp;
} mqtt_timestamp_t;

typedef struct {
    char device_id[16];
    uint32_t sample_block;
    uint32_t sample_count;
    int32_t latency_us[SAMPLES_PER_BLOCK];
} mqtt_latency_report_t;
#pragma pack(pop)

// Finite state machine states for the benchmark test cycle
typedef enum
{
    STATE_IDLE,
    STATE_WAITING_ACTIVATION,
    STATE_RUNNING,
    STATE_COOLDOWN,
    STATE_WAITING_REPORT_SYNC,
    STATE_REPORTING,
    STATE_WAITING_FINISH
} app_state_t;

volatile app_state_t current_state = STATE_IDLE;

uint32_t current_hz = 50;
uint32_t target_samples = 0;
uint32_t qos_mode = 0;
uint32_t start_total_time = 0;
uint32_t start_idle_time = 0;

char device_namespace[16];
uint8_t my_mac_tail = 0;

static int32_t *latency_buffer = NULL;
static size_t current_sample_index = 0;
volatile bool mqtt_connected = false;
volatile bool feedback_authorized = false;
volatile TickType_t activation_start_time = 0;

static EventGroupHandle_t wifi_event_group;

// MQTT Client Handle
esp_mqtt_client_handle_t mqtt_client = NULL;

// Topic Strings
static char topic_tx_discovery[] = "esp32/discovery";
static char topic_tx_timestamp_be[] = "esp32/timestamp_be";
static char topic_tx_timestamp_rel[] = "esp32/timestamp_rel";
static char topic_tx_feedback[] = "esp32/feedback";

static char topic_rx_activate[64];
static char topic_rx_timestamp_be[64];
static char topic_rx_timestamp_rel[64];

/*
 * Handles Wi-Fi connection and IP events, triggering reconnections
 * and updating the synchronization event group bit.
 */
static void wifi_event_handler(void* arg, esp_event_base_t event_base, int32_t event_id, void* event_data)
{
    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START)
    {
        esp_wifi_connect();
    }
    else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED)
    {
        ESP_LOGW("WIFI", "Disconnected from AP. Retrying...");
        esp_wifi_connect();
        xEventGroupClearBits(wifi_event_group, WIFI_CONNECTED_BIT);
    }
    else if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP)
    {
        xEventGroupSetBits(wifi_event_group, WIFI_CONNECTED_BIT);
    }
}

/*
 * Connects to the Wi-Fi network and waits until the device
 * receives an IP address from the router.
 */
static void wifi_init(void)
{
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    wifi_event_group = xEventGroupCreate();

    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_create_default_wifi_sta();

    esp_event_handler_instance_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL, NULL);
    esp_event_handler_instance_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, NULL, NULL);

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    wifi_config_t wifi_config = {
        .sta = {
            .ssid = WIFI_SSID,
            .password = WIFI_PASS,
        },
    };
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_start());
    
    ESP_LOGI("WIFI", "Waiting for an IP address from the router...");
    xEventGroupWaitBits(wifi_event_group, WIFI_CONNECTED_BIT, pdFALSE, pdTRUE, portMAX_DELAY);
    ESP_LOGI("WIFI", "IP address received. Network ready.");
}

/*
 * Reads FreeRTOS runtime counters to obtain total execution ticks
 * and the combined idle ticks across both CPU cores.
 */
void capture_cpu_snapshot(uint32_t *total_time, uint32_t *idle_time)
{
    UBaseType_t array_size = uxTaskGetNumberOfTasks();
    TaskStatus_t *task_status_array = malloc(array_size * sizeof(TaskStatus_t));

    if (task_status_array == NULL) {
        return;
    }

    uint32_t current_total = 0;
    array_size = uxTaskGetSystemState(task_status_array, array_size, &current_total);

    *total_time = current_total;
    *idle_time = 0;

    for (UBaseType_t i = 0; i < array_size; i++) {
        // Sum execution time of IDLE tasks across Core 0 and Core 1
        if (strncmp(task_status_array[i].pcTaskName, "IDLE", 4) == 0) {
            *idle_time += task_status_array[i].ulRunTimeCounter;
        }
    }

    free(task_status_array);
}

/*
 * Reboots the ESP32 after a delay derived from the MAC address
 * to prevent simultaneous reconnection bursts to the network.
 */
void staggered_esp_restart(void)
{
    uint32_t cpu_stagger_ms = (my_mac_tail % 5) * 500;
    vTaskDelay(pdMS_TO_TICKS(cpu_stagger_ms));
    esp_wifi_disconnect();
    vTaskDelay(pdMS_TO_TICKS(100));
    esp_restart();
}

/*
 * Handles MQTT connection state and processes incoming messages.
 */
static void mqtt_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data)
{
    esp_mqtt_event_handle_t event = event_data;

    switch (event->event_id) {
        case MQTT_EVENT_CONNECTED:
        {
            ESP_LOGI(device_namespace, "Connected to MQTT Broker.");
            mqtt_connected = true;

            char debug_msg[64];
            snprintf(debug_msg, sizeof(debug_msg), "%s reconnected. Reset reason: %d", device_namespace, esp_reset_reason());
            esp_mqtt_client_publish(mqtt_client, "esp32/debug", debug_msg, 0, 1, 0);

            // Subscribe to all topics required for the benchmark
            esp_mqtt_client_subscribe(mqtt_client, "pc/wakeup", 1);
            esp_mqtt_client_subscribe(mqtt_client, "pc/test_finished", 1);
            esp_mqtt_client_subscribe(mqtt_client, topic_rx_activate, 1);
            esp_mqtt_client_subscribe(mqtt_client, topic_rx_timestamp_be, 0);
            esp_mqtt_client_subscribe(mqtt_client, topic_rx_timestamp_rel, 1);
            esp_mqtt_client_subscribe(mqtt_client, "pc/request_feedback", 1);
            break;
        }

        // Incoming message received: dispatch payload based on subscribed topic
        case MQTT_EVENT_DATA:
        {
            // Wake-up command: announce device presence on discovery topic
            if (strncmp(event->topic, "pc/wakeup", event->topic_len) == 0) 
            {
                if (current_state != STATE_IDLE) return;

                ESP_LOGI(device_namespace, "Wake up received.");
                activation_start_time = xTaskGetTickCount();
                current_state = STATE_WAITING_ACTIVATION;
                
                esp_mqtt_client_publish(mqtt_client, topic_tx_discovery, device_namespace, strlen(device_namespace), 1, 0);
            }
            // Activation command: parse test parameters and start benchmark execution
            else if (strncmp(event->topic, topic_rx_activate, event->topic_len) == 0)
            {
                if (current_state != STATE_WAITING_ACTIVATION) return;

                char payload_str[64] = {0};
                strncpy(payload_str, event->data, event->data_len);
                
                // Parse test parameters: frequency, target samples, and QoS level
                sscanf(payload_str, "%u,%u,%u", &current_hz, &target_samples, &qos_mode);
                
                if (target_samples > MAX_SAMPLES) target_samples = MAX_SAMPLES;
                
                ESP_LOGI(device_namespace, "Received Config: %u Hz | %u Samples | QoS: %u", 
                        (unsigned int)current_hz, (unsigned int)target_samples, (unsigned int)qos_mode);
                
                current_state = STATE_RUNNING;
            }
            // Echoed timestamp: calculate round-trip latency and record sample in buffer
            else if (strncmp(event->topic, topic_rx_timestamp_be, event->topic_len) == 0 || 
                     strncmp(event->topic, topic_rx_timestamp_rel, event->topic_len) == 0)
            {
                if (event->data_len != sizeof(mqtt_timestamp_t)) return;

                int64_t arrive_time = esp_timer_get_time();
                mqtt_timestamp_t *rx_msg = (mqtt_timestamp_t *)event->data;
                int64_t total_latency = arrive_time - rx_msg->timestamp;
                
                if (current_sample_index < target_samples) 
                {
                    latency_buffer[current_sample_index++] = (int32_t)total_latency;
                }
            }
            // Test completion signal: trigger staggered device reboot
            else if (strncmp(event->topic, "pc/test_finished", event->topic_len) == 0)
            {
                ESP_LOGW(device_namespace, "Test finished/aborted by PC. Restarting hardware...");
                staggered_esp_restart();
            }
            // Feedback request: authorize data upload to host PC
            else if (strncmp(event->topic, "pc/request_feedback", event->topic_len) == 0)
            {
                if (current_state == STATE_WAITING_REPORT_SYNC || current_state == STATE_COOLDOWN)
                {
                    feedback_authorized = true;
                }
            }
            break;
        }

        // Clear connection flag when broker disconnects
        case MQTT_EVENT_DISCONNECTED:
        {
            ESP_LOGW(device_namespace, "Disconnected from MQTT Broker.");
            mqtt_connected = false;
            break;
        }
        default:
        {
            break;
        }
    }
}

/*
 * Main benchmark task. Runs the state machine controlling transmission
 * timing, CPU utilization calculation, and latency data reporting.
 */
void mqtt_task(void *pvParameters)
{
    // Initialize outgoing messages with device identifier
    mqtt_timestamp_t msg_tms = {0};
    strncpy(msg_tms.device_id, device_namespace, 16);

    mqtt_latency_report_t feedback_msg = {0};
    strncpy(feedback_msg.device_id, device_namespace, 16);

    TickType_t state_timeout_start = 0;

    while (1)
    {
        // Suspend execution and reset to IDLE if broker connection drops
        if (!mqtt_connected)
        {
            ESP_LOGW(device_namespace, "Waiting for MQTT Broker to reconnect...");
            current_state = STATE_IDLE;
            vTaskDelay(pdMS_TO_TICKS(2000));
            continue;
        }

        switch (current_state)
        {
            // Awaits wake-up signal from host PC
            case STATE_IDLE:
            {
                vTaskDelay(pdMS_TO_TICKS(25));
                break;
            }

            // Awaits benchmark configuration parameters from host PC
            case STATE_WAITING_ACTIVATION:
            {
                // Reboot if activation parameters are not received within 12 seconds
                if ((xTaskGetTickCount() - activation_start_time) > pdMS_TO_TICKS(12000))
                {
                    staggered_esp_restart();
                }
                
                vTaskDelay(pdMS_TO_TICKS(25));
                break;
            }
        
            // Executes benchmark transmission loop and computes execution metrics
            case STATE_RUNNING:
            {
                current_sample_index = 0;
                TickType_t xLastWakeTime = xTaskGetTickCount();
                
                // Calculate delay between messages based on target frequency
                const TickType_t xFrequency = pdMS_TO_TICKS(1000 / current_hz);

                char *active_topic = (qos_mode == 0) ? topic_tx_timestamp_be : topic_tx_timestamp_rel;

                // Capture CPU snapshot before starting transmission loop
                capture_cpu_snapshot(&start_total_time, &start_idle_time);
                
                int64_t loop_start_time_us = esp_timer_get_time();

                for (int i = 0; i < target_samples; i++)
                {
                    if (current_state != STATE_RUNNING) break;

                    msg_tms.timestamp = esp_timer_get_time();
                    
                    esp_mqtt_client_publish(mqtt_client, active_topic, (const char*)&msg_tms, sizeof(mqtt_timestamp_t), qos_mode, 0);
                    
                    vTaskDelayUntil(&xLastWakeTime, xFrequency);
                }

                if (current_state == STATE_RUNNING)
                {
                    int64_t loop_end_time_us = esp_timer_get_time();
                    int32_t loop_duration_ms = (int32_t)((loop_end_time_us - loop_start_time_us) / 1000);

                    uint32_t end_total_time = 0;
                    uint32_t end_idle_time = 0;

                    // Capture CPU snapshot after loop and stagger reporting to avoid network collision
                    capture_cpu_snapshot(&end_total_time, &end_idle_time);
                    
                    uint32_t cpu_stagger_ms = 2000 + ((my_mac_tail % 5) * 200);
                    vTaskDelay(pdMS_TO_TICKS(cpu_stagger_ms));

                    uint32_t delta_total = end_total_time - start_total_time;
                    uint32_t delta_idle = end_idle_time - start_idle_time;

                    float cpu_load = 0.0f;
                    if (delta_total > 0) 
                    {
                        // Calculate CPU load across both cores
                        cpu_load = 100.0 - (((float)delta_idle / ((float)delta_total * 2.0)) * 100.0);
                    }

                    ESP_LOGI(device_namespace, "Execution CPU Load: %.2f%%\n", cpu_load);
                    
                    // Block 998: contains CPU load and execution duration
                    feedback_msg.sample_block = 998;
                    feedback_msg.sample_count = 2;

                    feedback_msg.latency_us[0] = (int32_t)(cpu_load * 100.0);
                    feedback_msg.latency_us[1] = loop_duration_ms;

                    size_t payload_size = 24 + (2 * sizeof(int32_t));
                    
                    int msg_id = -1;
                    TickType_t start_pub_time = xTaskGetTickCount();

                    while (msg_id == -1 && current_state == STATE_RUNNING)
                    {
                        // Reboot if telemetry delivery fails for more than 10 seconds
                        if ((xTaskGetTickCount() - start_pub_time) > pdMS_TO_TICKS(10000))
                        {
                            ESP_LOGE(device_namespace, "Communication timeout. Restarting device...");
                            staggered_esp_restart();
                        }

                        msg_id = esp_mqtt_client_publish(
                            mqtt_client, 
                            topic_tx_feedback, 
                            (const char*)&feedback_msg, 
                            payload_size, 
                            1, 
                            0
                        );

                        if (msg_id == -1)
                        {
                            vTaskDelay(pdMS_TO_TICKS(10));
                        }
                    }
                
                    // Wait for incoming messages before report phase
                    current_state = STATE_COOLDOWN;
                }
                break;
            }

            // Resets authorization flag and transitions to report synchronization
            case STATE_COOLDOWN:
            {
                feedback_authorized = false;
                current_state = STATE_WAITING_REPORT_SYNC;
                activation_start_time = xTaskGetTickCount();
                break;
            }

            // Awaits host authorization before uploading benchmark results
            case STATE_WAITING_REPORT_SYNC:
            {
                if (feedback_authorized)
                {
                    // Allow network buffers to clear before upload
                    vTaskDelay(pdMS_TO_TICKS(2000));

                    // Stagger upload start time by MAC address to prevent network congestion
                    uint32_t stagger_delay_ms = (my_mac_tail % 5) * 500;
                    vTaskDelay(pdMS_TO_TICKS(stagger_delay_ms));
                    
                    current_state = STATE_REPORTING;
                }
                else
                {
                    // Reboot if authorization is not received within 4 minutes
                    if ((xTaskGetTickCount() - activation_start_time) > pdMS_TO_TICKS(240000))
                    {
                        ESP_LOGE(device_namespace, "Timeout waiting for feedback request. Restarting device...");
                        staggered_esp_restart();
                    }

                    static TickType_t last_retry_time = 0;

                    // Resend telemetry Block 998 every 5 seconds until authorized
                    if ((xTaskGetTickCount() - last_retry_time) > pdMS_TO_TICKS(5000))
                    {
                        last_retry_time = xTaskGetTickCount();
                        ESP_LOGW(device_namespace, "PC feedback request not received. Resending Block 998...");
                        
                        size_t payload_size = 24 + (2 * sizeof(int32_t));
                        esp_mqtt_client_publish(mqtt_client, topic_tx_feedback, (const char*)&feedback_msg, payload_size, 1, 0);
                    }

                    vTaskDelay(pdMS_TO_TICKS(50));
                }
                break;
            }

            // Sends collected latency data in blocks to the host PC
            case STATE_REPORTING:
            {
                // Calculate total blocks needed for all samples
                uint32_t total_blocks = (current_sample_index + SAMPLES_PER_BLOCK - 1) / SAMPLES_PER_BLOCK;

                for (uint32_t block = 0; block < total_blocks; block++)
                {
                    if (current_state != STATE_REPORTING)
                        break;

                    size_t start_idx = block * SAMPLES_PER_BLOCK;
                    size_t samples_this_block = SAMPLES_PER_BLOCK;

                    if (start_idx + SAMPLES_PER_BLOCK > current_sample_index)
                    {
                        samples_this_block = current_sample_index - start_idx;
                    }

                    feedback_msg.sample_block = block;
                    feedback_msg.sample_count = samples_this_block;

                    memcpy(feedback_msg.latency_us,
                        &latency_buffer[start_idx],
                        samples_this_block * sizeof(int32_t));

                    // 24-byte header (device_id, block index, sample count) plus sample data
                    size_t payload_size = 24 + (samples_this_block * sizeof(int32_t));

                    int msg_id = -1;

                    while (msg_id == -1 && current_state == STATE_REPORTING)
                    {
                        msg_id = esp_mqtt_client_publish(
                            mqtt_client,
                            topic_tx_feedback,
                            (const char*)&feedback_msg,
                            payload_size,
                            1,
                            0
                        );

                        if (msg_id == -1)
                        {
                            vTaskDelay(pdMS_TO_TICKS(10));
                        }
                    }

                    if (current_state == STATE_REPORTING)
                    {
                        // Delay between blocks to avoid dropping packets on the receiver
                        vTaskDelay(pdMS_TO_TICKS(150));
                    }
                }

                if (current_state == STATE_REPORTING)
                {
                    // Block 9999: signals completion of data upload
                    feedback_msg.sample_block = 9999;
                    feedback_msg.sample_count = 0;

                    int msg_id = -1;
                    TickType_t start_pub_time_end = xTaskGetTickCount();

                    while (msg_id == -1 && current_state == STATE_REPORTING)
                    {
                        // Reboot if completion signal fails for more than 10 seconds
                        if ((xTaskGetTickCount() - start_pub_time_end) > pdMS_TO_TICKS(10000)) {
                            ESP_LOGE(device_namespace, "Communication timeout. Restarting device...");
                            staggered_esp_restart();
                        }

                        msg_id = esp_mqtt_client_publish(mqtt_client, topic_tx_feedback, (const char*)&feedback_msg, 24, 1, 0);
                        if (msg_id == -1)
                        { 
                            vTaskDelay(pdMS_TO_TICKS(10));
                        }
                    }

                    if (current_state == STATE_REPORTING)
                    {
                        current_state = STATE_WAITING_FINISH;
                        state_timeout_start = xTaskGetTickCount();
                    }
                }

                break;
            }


            // Awaits test finished signal from host PC
            case STATE_WAITING_FINISH:
            {
                // Reboot if completion signal is not received within 2 minutes
                if ((xTaskGetTickCount() - state_timeout_start) > pdMS_TO_TICKS(120000))
                {
                    ESP_LOGE(device_namespace, "Timeout waiting for /test_finished. Restarting device...");
                    staggered_esp_restart();
                }

                vTaskDelay(pdMS_TO_TICKS(25));
                break;
            }
        }
    }
}

void app_main(void)
{
    // Initialize the native Wi-Fi interface and connect to the router
    wifi_init();

    // Disable Wi-Fi power save mode to reduce communication latency
    ESP_ERROR_CHECK(esp_wifi_set_ps(WIFI_PS_NONE));
    
    // Increase Wi-Fi signal strength by setting the maximum TX power to 19.5 dBm.
    esp_wifi_set_max_tx_power(78);

    // Get the MAC address to generate a unique device ID
    uint8_t mac[6];
    esp_err_t err = esp_read_mac(mac, ESP_MAC_WIFI_STA);

    if (err != ESP_OK) 
    {
        ESP_LOGE("BOOT", "Failed to read MAC: %s", esp_err_to_name(err));
        return;
    }

    my_mac_tail = mac[5];

    // Format the device namespace using the last two bytes of the MAC address
    snprintf(device_namespace, sizeof(device_namespace), "ESP32_%02X%02X", mac[4], mac[5]);
    ESP_LOGI("BOOT", "Device namespace: %s", device_namespace);

    // Format the topic names dynamically based on the device namespace
    snprintf(topic_rx_activate, sizeof(topic_rx_activate), "pc/%s/activate", device_namespace);
    snprintf(topic_rx_timestamp_be, sizeof(topic_rx_timestamp_be), "pc/%s/timestamp_be", device_namespace);
    snprintf(topic_rx_timestamp_rel, sizeof(topic_rx_timestamp_rel), "pc/%s/timestamp_rel", device_namespace);

    // Allocate memory for the latency data buffer
    latency_buffer = (int32_t *)malloc(MAX_SAMPLES * sizeof(int32_t));

    if (latency_buffer == NULL) 
    {
        ESP_LOGE(device_namespace, "CRITICAL ERROR: Insufficient RAM for %d samples.", MAX_SAMPLES);
        return;
    }

    // Initialize and start the MQTT Client
    esp_mqtt_client_config_t mqtt_cfg = {
        .uri = MQTT_BROKER_URI,
        .out_buffer_size = 16384,
        .buffer_size = 16384,
        .disable_auto_reconnect = false,
        .network_timeout_ms = 10000,
        .reconnect_timeout_ms = 3000,
        .keepalive = 600,
        .task_prio = 10,
        .disable_clean_session = false
    };
    mqtt_client = esp_mqtt_client_init(&mqtt_cfg);
    esp_mqtt_client_register_event(mqtt_client, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL);
    esp_mqtt_client_start(mqtt_client);

    while (!mqtt_connected)
    {
        if (current_state != STATE_IDLE && current_state != STATE_WAITING_ACTIVATION)
            {
                ESP_LOGE(device_namespace, "Broker connection lost mid-test. Restarting to purge LwIP buffers...");
                staggered_esp_restart();
            }

            ESP_LOGW(device_namespace, "Waiting for MQTT Broker to reconnect...");
            current_state = STATE_IDLE;
            vTaskDelay(pdMS_TO_TICKS(2000));
            continue;
    }

    // Create the FreeRTOS task to run the state machine
    BaseType_t xResult = xTaskCreatePinnedToCore(mqtt_task, "mqtt_task", 8192, NULL, 4, NULL, 1);
    if (xResult != pdPASS) return;

    ESP_LOGI(device_namespace, "MQTT node initialized. Entering idle state.");
}