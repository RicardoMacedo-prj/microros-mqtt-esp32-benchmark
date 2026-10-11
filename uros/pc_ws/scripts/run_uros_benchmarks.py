#!/usr/bin/env python3
import argparse
import os
import queue
import re
import signal
import subprocess
import threading
import time


def kill_process_group(proc, name):
    """Terminate a process and its child processes within the assigned process group."""
    if proc is None or proc.poll() is not None:
        return

    print(f"[{name}] Stopping process...")

    try:
        pgid = os.getpgid(proc.pid)
        # Send interrupt signal to the entire process group
        os.killpg(pgid, signal.SIGINT)
        proc.wait(timeout=5)

    except ProcessLookupError:
        pass

    except subprocess.TimeoutExpired:
        print(f"[{name}] Timeout expired. Forcing shutdown...")

        try:
            # Force termination if the process group remains active
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass

        proc.wait()


def enqueue_output(out, q):
    """Read stdout asynchronously and enqueue lines into a thread-safe queue."""
    # Drain stream continuously to prevent OS pipe buffer saturation
    for line in iter(out.readline, ""):
        q.put(line)
    out.close()


def main():
    """Run the micro-ROS benchmark test matrix and save results to a CSV file."""
    # Command-line interface
    parser = argparse.ArgumentParser(
        description="micro-ROS hardware-in-the-loop benchmark test runner"
    )

    parser.add_argument(
        "--messages",
        type=int,
        default=10000,
        help="Number of messages per test (default: 10000)"
    )
    parser.add_argument(
        "--hz",
        type=int,
        help="Message publishing rate in Hz"
    )
    parser.add_argument(
        "--nodes",
        type=int,
        help="Number of active ESP32 nodes"
    )
    parser.add_argument(
        "--qos",
        type=int,
        choices=[0, 1],
        help="Quality of Service level (0: Best Effort, 1: Reliable)"
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=5,
        help="Number of repetitions for each test configuration (default: 5)"
    )

    args = parser.parse_args()

    # Reject non-positive values before starting execution
    if args.messages <= 0 or args.runs <= 0:
        parser.error("--messages and --runs must be strictly positive integers")
    if args.hz is not None and args.hz <= 0:
        parser.error("--hz must be a strictly positive integer")
    if args.nodes is not None and args.nodes <= 0:
        parser.error("--nodes must be a strictly positive integer")

    # Test configurations
    test_matrix = [
        {"messages": 10000, "hz": 50,  "nodes": 1, "qos_mode": 0},
        {"messages": 10000, "hz": 100, "nodes": 1, "qos_mode": 0},
        {"messages": 10000, "hz": 200, "nodes": 1, "qos_mode": 0},

        {"messages": 10000, "hz": 50,  "nodes": 3, "qos_mode": 0},
        {"messages": 10000, "hz": 100, "nodes": 3, "qos_mode": 0},
        {"messages": 10000, "hz": 200, "nodes": 3, "qos_mode": 0},

        {"messages": 10000, "hz": 50,  "nodes": 5, "qos_mode": 0},
        {"messages": 10000, "hz": 100, "nodes": 5, "qos_mode": 0},
        {"messages": 10000, "hz": 200, "nodes": 5, "qos_mode": 0},

        {"messages": 10000, "hz": 50,  "nodes": 1, "qos_mode": 1},
        {"messages": 10000, "hz": 100, "nodes": 1, "qos_mode": 1},
        {"messages": 10000, "hz": 200, "nodes": 1, "qos_mode": 1},

        {"messages": 10000, "hz": 50,  "nodes": 3, "qos_mode": 1},
        {"messages": 10000, "hz": 100, "nodes": 3, "qos_mode": 1},
        {"messages": 10000, "hz": 200, "nodes": 3, "qos_mode": 1},

        {"messages": 10000, "hz": 50,  "nodes": 5, "qos_mode": 1},
        {"messages": 10000, "hz": 100, "nodes": 5, "qos_mode": 1},
        {"messages": 10000, "hz": 200, "nodes": 5, "qos_mode": 1}
    ]

    # Single configuration execution if all parameters are specified
    if args.hz is not None and args.nodes is not None and args.qos is not None:
        print("[INIT] Single test mode: using targeted configuration.")

        test_matrix = [{
            "messages": args.messages,
            "hz": args.hz,
            "nodes": args.nodes,
            "qos_mode": args.qos
        }]

    else:
        print("[INIT] Filtered test mode: applying selected filters.")

        if args.hz is not None:
            test_matrix = [
                t for t in test_matrix
                if t["hz"] == args.hz
            ]

        if args.nodes is not None:
            test_matrix = [
                t for t in test_matrix
                if t["nodes"] == args.nodes
            ]

        if args.qos is not None:
            test_matrix = [
                t for t in test_matrix
                if t["qos_mode"] == args.qos
            ]

        # Override default message count for all filtered tests
        for t in test_matrix:
            t["messages"] = args.messages

        if not test_matrix:
            print("[ERROR] No test configuration matches the selected filters.")
            return

    repeats = args.runs

    # Resolve output path relative to script location
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    results_dir = os.path.join(base_dir, "results")
    os.makedirs(results_dir, exist_ok=True)
    csv_filename = os.path.join(results_dir, "uros_benchmarks_log.csv")

    # Create CSV header if the log file does not exist
    if not os.path.exists(csv_filename):
        with open(csv_filename, "w", encoding="utf-8") as f:
            f.write(
                "test_id,run_num,total_messages,hz,nodes,qos_mode,"
                "failure_count,device_id,block_id,cpu_load,throughput,"
                "exec_time_ms,latencies_us\n"
            )

    # Regex pattern matching telemetry output from the C++ relay node
    log_pattern = re.compile(
        r"Device (ESP32_[A-F0-9]+) \| "
        r"Block (\d+) \| "
        r"CPU: ([\d\.]+) \| "
        r"Throughput: ([\d\.]+) \| "
        r"ExecTime: (\d+) \| "
        r"Samples: \d+ \| "
        r"Latencies: \[(.*?)\]"
    )

    # Configure environment variables for unbuffered ROS 2 and Python stdout
    env = os.environ.copy()
    env["RCUTILS_COLORIZED_OUTPUT"] = "0"
    env["RCUTILS_LOGGING_USE_STDOUT"] = "1"
    env["RCUTILS_LOGGING_BUFFERED_STREAM"] = "0"
    env["PYTHONUNBUFFERED"] = "1"

    # Terminate lingering processes from prior runs
    subprocess.run(
        ["killall", "-9", "micro_ros_agent", "relay_node"],
        stderr=subprocess.DEVNULL
    )

    print("[BENCHMARK] HIL benchmark runner started.")

    with open(csv_filename, "a", encoding="utf-8") as csv_file:

        for idx, test in enumerate(test_matrix):
            run_num = 1

            while run_num <= repeats:
                qos_name = "Best Effort" if test["qos_mode"] == 0 else "Reliable"
                failures_this_run = 0
                run_success = False

                # Clear Fast DDS shared memory segments to prevent stale participant discovery
                subprocess.run(
                    "rm -rf /dev/shm/*fastrtps*",
                    shell=True,
                    stderr=subprocess.DEVNULL
                )

                # Repeat configuration until a run completes successfully
                while not run_success:
                    print("\n=======================================================")
                    print(
                        f"[TEST {idx + 1}/{len(test_matrix)}] "
                        f"{test['messages']} messages | "
                        f"{test['hz']} Hz | "
                        f"{test['nodes']} nodes | "
                        f"QoS: {qos_name}"
                    )
                    print(
                        f"--- RUN {run_num} OF {repeats} | "
                        f"Failures: {failures_this_run} ---"
                    )
                    print("=======================================================")

                    # Terminate lingering processes before spawning new instances
                    subprocess.run(
                        ["killall", "-9", "micro_ros_agent", "relay_node"],
                        stderr=subprocess.DEVNULL
                    )

                    agent_process = None
                    relay_process = None

                    try:
                        # Start micro-ROS Agent over UDP on port 8888
                        agent_cmd = [
                            "ros2",
                            "run",
                            "micro_ros_agent",
                            "micro_ros_agent",
                            "udp4",
                            "--port",
                            "8888"
                        ]
                        agent_process = subprocess.Popen(
                            agent_cmd,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                            preexec_fn=os.setsid
                        )

                        # Start C++ relay node
                        relay_cmd = [
                            "ros2",
                            "run",
                            "pc_node",
                            "relay_node"
                        ]
                        relay_process = subprocess.Popen(
                            relay_cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            text=True,
                            env=env,
                            preexec_fn=os.setsid
                        )

                        # Allow ESP32 nodes to reboot and establish UDP sessions
                        print(
                            "[RUNNER] Waiting 20 seconds for the ESP32 "
                            "devices to reconnect..."
                        )
                        time.sleep(20)

                        # Configure runtime parameters on the relay node
                        subprocess.run(
                            [
                                "ros2",
                                "param",
                                "set",
                                "/relay_node",
                                "number_of_devices_to_activate",
                                str(test["nodes"])
                            ],
                            check=True,
                            stdout=subprocess.DEVNULL,
                            env=env
                        )

                        subprocess.run(
                            [
                                "ros2",
                                "param",
                                "set",
                                "/relay_node",
                                "current_hz",
                                str(test["hz"])
                            ],
                            check=True,
                            stdout=subprocess.DEVNULL,
                            env=env
                        )

                        subprocess.run(
                            [
                                "ros2",
                                "param",
                                "set",
                                "/relay_node",
                                "expected_total_samples",
                                str(test["messages"])
                            ],
                            check=True,
                            stdout=subprocess.DEVNULL,
                            env=env
                        )

                        subprocess.run(
                            [
                                "ros2",
                                "param",
                                "set",
                                "/relay_node",
                                "qos_mode",
                                str(test["qos_mode"])
                            ],
                            check=True,
                            stdout=subprocess.DEVNULL,
                            env=env
                        )

                        # Start benchmark execution
                        print("[RUNNER] Starting the test...")

                    # Trigger benchmark execution via ROS 2 service call
                        print("[RUNNER] Starting the test...")
                        subprocess.run(
                            [
                                "ros2",
                                "service",
                                "call",
                                "/start_test",
                                "std_srvs/srv/Trigger",
                                "{}"
                            ],
                            check=True,
                            stdout=subprocess.DEVNULL,
                            env=env
                        )

                        last_msg_time = time.monotonic()
                        ram_buffer = []
                        test_finished_flag = False

                        # Timeout with expected execution time plus 600 seconds margin
                        base_exec_time = test["messages"] / test["hz"]
                        current_timeout_limit = base_exec_time + 600

                        # Start asynchronous reader thread to drain stdout
                        output_queue = queue.Queue()
                        reader_thread = threading.Thread(
                            target=enqueue_output,
                            args=(relay_process.stdout, output_queue),
                            daemon=True
                        )
                        reader_thread.start()

                        # Read and process output lines from the C++ relay node
                        while True:
                            try:
                                # Read next line from queue with a 1-second timeout
                                line = output_queue.get(timeout=1.0)
                                clean_line = line.strip()

                                # Reset inactivity timer when new output arrives
                                last_msg_time = time.monotonic()

                                match = log_pattern.search(clean_line)
                                if match:
                                    # Reduce inactivity timeout to 120s once telemetry reception starts
                                    current_timeout_limit = 120.0

                                    device_id = match.group(1)
                                    block_id = match.group(2)
                                    cpu_load = match.group(3)
                                    throughput = match.group(4)
                                    exec_time = match.group(5)
                                    latencies = match.group(6)

                                    # Buffer row in memory until the entire run succeeds
                                    csv_row = (
                                        f"{idx + 1},"
                                        f"{run_num},"
                                        f"{test['messages']},"
                                        f"{test['hz']},"
                                        f"{test['nodes']},"
                                        f"{test['qos_mode']},"
                                        f"{failures_this_run},"
                                        f"{device_id},"
                                        f"{block_id},"
                                        f"{cpu_load},"
                                        f"{throughput},"
                                        f"{exec_time},"
                                        f"\"{latencies}\"\n"
                                    )

                                    ram_buffer.append(csv_row)
                                    print(f"  [OK] Block {block_id} from {device_id} ({len(ram_buffer)} received)")

                                elif clean_line:
                                    print(f"  -> {clean_line}")

                                # Check completion messages printed by the relay node
                                if "Test finished. All devices sent their data." in clean_line:
                                    test_finished_flag = True

                                if "Relay node is ready for a new experiment." in clean_line:
                                    if test_finished_flag:
                                        time.sleep(3)
                                        run_success = True
                                    break

                            except queue.Empty:
                                # Abort run if no data is received within the timeout limit
                                if time.monotonic() - last_msg_time > current_timeout_limit:
                                    print(
                                        f"\n[ERROR] No data received for more "
                                        f"than {current_timeout_limit:.1f} seconds."
                                    )
                                    break

                    finally:
                        # Ensure all processes are terminated before next attempt
                        kill_process_group(relay_process, "C++ Relay Node")
                        kill_process_group(agent_process, "Micro-ROS Agent")

                    # Check whether the run was successful
                    if run_success:
                        print(
                            "[RUNNER] Run completed successfully. "
                            "Saving results to CSV..."
                        )

                        for row in ram_buffer:
                            csv_file.write(row)

                        csv_file.flush()

                        print(
                            f"[RUNNER] Run {run_num}/{repeats} completed. "
                            "Waiting 5 seconds..."
                        )

                        time.sleep(5)
                        run_num += 1

                    else:
                        print(
                            "[RUNNER] Run failed. "
                            "Discarding the collected data and retrying..."
                        )

                        failures_this_run += 1

                        # Allow network sockets to close and ESP32 nodes to reconnect
                        time.sleep(125)

    print("\n[BENCHMARK] All tests completed.")


if __name__ == "__main__":
    main()