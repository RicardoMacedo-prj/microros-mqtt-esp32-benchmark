#ifndef MQTT_BRIDGE__PROTOCOL_HPP_
#define MQTT_BRIDGE__PROTOCOL_HPP_

#include <cstddef>
#include <cstdint>

namespace mqtt_bridge
{
    // Maximum device identifier length in bytes
    constexpr std::size_t DEVICE_ID_SIZE = 16;

    // Maximum number of latency samples per feedback packet
    constexpr std::size_t MAX_LATENCY_SAMPLES = 100;

    /*
     * Binary format of an MQTT timestamp packet:
     *
     * Offset   Size (bytes)   Field
     * 0        16             device_id
     * 16       8              timestamp (int64_t)
     */
    constexpr std::size_t TIMESTAMP_PAYLOAD_SIZE =
      DEVICE_ID_SIZE + sizeof(std::int64_t);

    /*
     * Binary format of an MQTT latency report packet:
     *
     * Offset   Size (bytes)   Field
     * 0        16             device_id
     * 16       4              sample_block
     * 20       4              sample_count
     * 24       N * 4          latency_us[]
     *
     * Where N is the number of 32-bit latency samples in the block.
     */

    constexpr std::size_t LATENCY_HEADER_SIZE =
      DEVICE_ID_SIZE +
      sizeof(std::uint32_t) +
      sizeof(std::uint32_t);

    constexpr std::size_t MAX_LATENCY_PAYLOAD_SIZE =
      LATENCY_HEADER_SIZE +
      (MAX_LATENCY_SAMPLES * sizeof(std::int32_t));

}  // namespace mqtt_bridge

#endif  // MQTT_BRIDGE__PROTOCOL_HPP_
