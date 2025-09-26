#include <cmath>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <fstream>
#include <iomanip>
#include <chrono>
#include <sstream>
#include <vector>
#include <string>
#include <algorithm>
#include <signal.h>

// DDS
#include <unitree/robot/channel/channel_publisher.hpp>
#include <unitree/robot/channel/channel_subscriber.hpp>

// IDL
#include <unitree/idl/hg/LowCmd_.hpp>
#include <unitree/idl/hg/LowState_.hpp>

#include <unitree/robot/b2/motion_switcher/motion_switcher_client.hpp>
using namespace unitree::robot::b2;

static const std::string HG_CMD_TOPIC = "rt/lowcmd";
static const std::string HG_STATE_TOPIC = "rt/lowstate";

using namespace unitree::common;
using namespace unitree::robot;

const int G1_NUM_MOTOR = 29;

// Global flag for graceful shutdown
volatile bool g_running = true;

void signalHandler(int signum) {
    std::cout << "\nInterrupt signal (" << signum << ") received. Stopping playback..." << std::endl;
    g_running = false;
}

template <typename T>
class DataBuffer {
 public:
  void SetData(const T &newData) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    data = std::make_shared<T>(newData);
  }

  std::shared_ptr<const T> GetData() {
    std::shared_lock<std::shared_mutex> lock(mutex);
    return data ? data : nullptr;
  }

  void Clear() {
    std::unique_lock<std::shared_mutex> lock(mutex);
    data = nullptr;
  }

 private:
  std::shared_ptr<T> data;
  std::shared_mutex mutex;
};

struct ImuState {
  std::array<float, 3> rpy = {};
  std::array<float, 3> omega = {};
};

struct MotorCommand {
  std::array<float, G1_NUM_MOTOR> q_target = {};
  std::array<float, G1_NUM_MOTOR> dq_target = {};
  std::array<float, G1_NUM_MOTOR> kp = {};
  std::array<float, G1_NUM_MOTOR> kd = {};
  std::array<float, G1_NUM_MOTOR> tau_ff = {};
};

struct MotorState {
  std::array<float, G1_NUM_MOTOR> q = {};
  std::array<float, G1_NUM_MOTOR> dq = {};
};

struct TrajectoryPoint {
  double timestamp;
  std::array<float, 14> arm_positions;  // joints 15-28
  std::array<float, 14> arm_velocities;
};

enum MotorType { GearboxS = 0, GearboxM = 1, GearboxL = 2 };

std::array<MotorType, G1_NUM_MOTOR> G1MotorType{
    // clang-format off
    // legs
    GearboxM, GearboxM, GearboxM, GearboxL, GearboxS, GearboxS,
    GearboxM, GearboxM, GearboxM, GearboxL, GearboxS, GearboxS,
    // waist
    GearboxM, GearboxS, GearboxS,
    // arms
    GearboxS, GearboxS, GearboxS, GearboxS, GearboxS, GearboxS, GearboxS,
    GearboxS, GearboxS, GearboxS, GearboxS, GearboxS, GearboxS, GearboxS
    // clang-format on
};

enum PRorAB { PR = 0, AB = 1 };

enum G1JointValidIndex {
  LeftShoulderPitch = 15,
  LeftShoulderRoll = 16,
  LeftShoulderYaw = 17,
  LeftElbow = 18,
  LeftWristRoll = 19,
  LeftWristPitch = 20,
  LeftWristYaw = 21,
  RightShoulderPitch = 22,
  RightShoulderRoll = 23,
  RightShoulderYaw = 24,
  RightElbow = 25,
  RightWristRoll = 26,
  RightWristPitch = 27,
  RightWristYaw = 28
};

inline uint32_t Crc32Core(uint32_t *ptr, uint32_t len) {
  uint32_t xbit = 0;
  uint32_t data = 0;
  uint32_t CRC32 = 0xFFFFFFFF;
  const uint32_t dwPolynomial = 0x04c11db7;
  for (uint32_t i = 0; i < len; i++) {
    xbit = 1 << 31;
    data = ptr[i];
    for (uint32_t bits = 0; bits < 32; bits++) {
      if (CRC32 & 0x80000000) {
        CRC32 <<= 1;
        CRC32 ^= dwPolynomial;
      } else
        CRC32 <<= 1;
      if (data & xbit) CRC32 ^= dwPolynomial;

      xbit >>= 1;
    }
  }
  return CRC32;
};

// Moderate control gains for smooth playback
float GetPlaybackKp(MotorType type) {
  switch (type) {
    case GearboxS:
      return 30;   // Moderate stiffness
    case GearboxM:
      return 30;
    case GearboxL:
      return 50;
    default:
      return 0;
  }
}

float GetPlaybackKd(MotorType type) {
  switch (type) {
    case GearboxS:
      return 5;    // Good damping for stability
    case GearboxM:
      return 5;
    case GearboxL:
      return 8;
    default:
      return 0;
  }
}

class G1ArmPlayback {
 private:
  double time_;
  double control_dt_;  // [2ms]
  double playback_start_time_;
  PRorAB mode_;
  uint8_t mode_machine_;
  
  std::vector<TrajectoryPoint> trajectory_;
  size_t current_trajectory_index_;
  bool playback_complete_;
  bool first_state_received_;
  bool ready_to_start_;
  
  DataBuffer<MotorState> motor_state_buffer_;
  DataBuffer<MotorCommand> motor_command_buffer_;
  DataBuffer<ImuState> imu_state_buffer_;

  ChannelPublisherPtr<unitree_hg::msg::dds_::LowCmd_> lowcmd_publisher_;
  ChannelSubscriberPtr<unitree_hg::msg::dds_::LowState_> lowstate_subscriber_;
  ThreadPtr command_writer_ptr_, control_thread_ptr_;

  std::shared_ptr<MotionSwitcherClient> msc;

 public:
  G1ArmPlayback(std::string networkInterface, std::string csv_filename)
      : time_(0.0),
        control_dt_(0.002),
        playback_start_time_(0.0),
        mode_(PR),
        mode_machine_(0),
        current_trajectory_index_(0),
        playback_complete_(false),
        first_state_received_(false),
        ready_to_start_(false) {
        
    // Load trajectory from CSV
    if (!LoadTrajectory(csv_filename)) {
      throw std::runtime_error("Failed to load trajectory from CSV file");
    }
    
    std::cout << "Loaded trajectory with " << trajectory_.size() << " points" << std::endl;
    std::cout << "Duration: " << trajectory_.back().timestamp << " seconds" << std::endl;
        
    ChannelFactory::Instance()->Init(0, networkInterface);

    msc.reset(new MotionSwitcherClient());
    msc->SetTimeout(5.0F);
    msc->Init();

    /*Shut down motion control-related service*/
    while(queryMotionStatus())
    {
        std::cout << "Try to deactivate the motion control-related service." << std::endl;
        int32_t ret = msc->ReleaseMode(); 
        if (ret == 0) {
            std::cout << "ReleaseMode succeeded." << std::endl;
        } else {
            std::cout << "ReleaseMode failed. Error code: " << ret << std::endl;
        }
        sleep(5);
    }

    // create publisher
    lowcmd_publisher_.reset(
        new ChannelPublisher<unitree_hg::msg::dds_::LowCmd_>(HG_CMD_TOPIC));
    lowcmd_publisher_->InitChannel();

    // create subscriber
    lowstate_subscriber_.reset(
        new ChannelSubscriber<unitree_hg::msg::dds_::LowState_>(
            HG_STATE_TOPIC));
    lowstate_subscriber_->InitChannel(
        std::bind(&G1ArmPlayback::LowStateHandler, this, std::placeholders::_1), 1);

    // create threads
    command_writer_ptr_ =
        CreateRecurrentThreadEx("command_writer", UT_CPU_ID_NONE, 2000,
                                &G1ArmPlayback::LowCommandWriter, this);
    control_thread_ptr_ = CreateRecurrentThreadEx(
        "control", UT_CPU_ID_NONE, 2000, &G1ArmPlayback::Control, this);
        
    std::cout << "G1 Arm Playback initialized." << std::endl;
    std::cout << "Waiting for robot state... Press Enter to start playback when ready." << std::endl;
  }

  ~G1ArmPlayback() {
    std::cout << "Playback stopped." << std::endl;
  }

  bool LoadTrajectory(const std::string& filename) {
    std::ifstream file(filename);
    if (!file.is_open()) {
      std::cerr << "Error: Could not open CSV file: " << filename << std::endl;
      return false;
    }
    
    std::string line;
    // Skip header
    if (!std::getline(file, line)) {
      std::cerr << "Error: Empty CSV file" << std::endl;
      return false;
    }
    
    trajectory_.clear();
    
    while (std::getline(file, line)) {
      if (line.empty()) continue;
      
      TrajectoryPoint point;
      std::stringstream ss(line);
      std::string cell;
      int col = 0;
      
      while (std::getline(ss, cell, ',')) {
        float value = std::stof(cell);
        
        if (col == 0) {
          point.timestamp = value;
        } else if (col >= 1 && col <= 14) {
          point.arm_positions[col-1] = value;
        } else if (col >= 15 && col <= 28) {
          point.arm_velocities[col-15] = value;
        }
        col++;
      }
      
      trajectory_.push_back(point);
    }
    
    file.close();
    
    if (trajectory_.empty()) {
      std::cerr << "Error: No trajectory data loaded" << std::endl;
      return false;
    }
    
    return true;
  }

  void LowStateHandler(const void *message) {
    unitree_hg::msg::dds_::LowState_ low_state =
        *(const unitree_hg::msg::dds_::LowState_ *)message;

    if (low_state.crc() !=
        Crc32Core((uint32_t *)&low_state,
                  (sizeof(unitree_hg::msg::dds_::LowState_) >> 2) - 1)) {
      std::cout << "low_state CRC Error" << std::endl;
      return;
    }

    // get motor state
    MotorState ms_tmp;
    for (int i = 0; i < G1_NUM_MOTOR; ++i) {
      ms_tmp.q.at(i) = low_state.motor_state()[i].q();
      ms_tmp.dq.at(i) = low_state.motor_state()[i].dq();
    }
    motor_state_buffer_.SetData(ms_tmp);

    // get imu state
    ImuState imu_tmp;
    imu_tmp.omega = low_state.imu_state().gyroscope();
    imu_tmp.rpy = low_state.imu_state().rpy();
    imu_state_buffer_.SetData(imu_tmp);

    // update mode machine
    if (mode_machine_ != low_state.mode_machine()) {
      if (mode_machine_ == 0)
        std::cout << "G1 type: " << unsigned(low_state.mode_machine())
                  << std::endl;
      mode_machine_ = low_state.mode_machine();
    }
    
    first_state_received_ = true;
  }

  void WaitForStart() {
    while (!first_state_received_ && g_running) {
      std::cout << "Waiting for robot state..." << std::endl;
      sleep(1);
    }
    
    if (!g_running) return;
    
    std::cout << "Robot state received. Press Enter to start playback..." << std::endl;
    std::cin.get();
    ready_to_start_ = true;
    playback_start_time_ = time_;
    std::cout << "Starting playback!" << std::endl;
  }

  TrajectoryPoint InterpolateTrajectory(double playback_time) {
    if (current_trajectory_index_ >= trajectory_.size()) {
      playback_complete_ = true;
      return trajectory_.back();
    }
    
    // Find the trajectory segment we're in
    while (current_trajectory_index_ < trajectory_.size() - 1 &&
           playback_time > trajectory_[current_trajectory_index_ + 1].timestamp) {
      current_trajectory_index_++;
    }
    
    if (current_trajectory_index_ >= trajectory_.size() - 1) {
      playback_complete_ = true;
      return trajectory_.back();
    }
    
    // Linear interpolation between current and next point
    const TrajectoryPoint& p1 = trajectory_[current_trajectory_index_];
    const TrajectoryPoint& p2 = trajectory_[current_trajectory_index_ + 1];
    
    double dt = p2.timestamp - p1.timestamp;
    if (dt <= 0) return p1;
    
    double alpha = (playback_time - p1.timestamp) / dt;
    alpha = std::max(0.0, std::min(1.0, alpha));
    
    TrajectoryPoint result;
    result.timestamp = playback_time;
    
    for (int i = 0; i < 14; ++i) {
      result.arm_positions[i] = p1.arm_positions[i] + alpha * (p2.arm_positions[i] - p1.arm_positions[i]);
      result.arm_velocities[i] = p1.arm_velocities[i] + alpha * (p2.arm_velocities[i] - p1.arm_velocities[i]);
    }
    
    return result;
  }

  void LowCommandWriter() {
    unitree_hg::msg::dds_::LowCmd_ dds_low_command;
    dds_low_command.mode_pr() = mode_;
    dds_low_command.mode_machine() = mode_machine_;

    const std::shared_ptr<const MotorCommand> mc =
        motor_command_buffer_.GetData();
    if (mc) {
      for (size_t i = 0; i < G1_NUM_MOTOR; i++) {
        dds_low_command.motor_cmd().at(i).mode() = 1;  // 1:Enable, 0:Disable
        dds_low_command.motor_cmd().at(i).tau() = mc->tau_ff.at(i);
        dds_low_command.motor_cmd().at(i).q() = mc->q_target.at(i);
        dds_low_command.motor_cmd().at(i).dq() = mc->dq_target.at(i);
        dds_low_command.motor_cmd().at(i).kp() = mc->kp.at(i);
        dds_low_command.motor_cmd().at(i).kd() = mc->kd.at(i);
      }

      dds_low_command.crc() = Crc32Core((uint32_t *)&dds_low_command,
                                        (sizeof(dds_low_command) >> 2) - 1);
      lowcmd_publisher_->Write(dds_low_command);
    }
  }

  void Control() {
    const std::shared_ptr<const MotorState> ms = motor_state_buffer_.GetData();
    
    if (!ms || !first_state_received_) return;

    time_ += control_dt_;
    
    MotorCommand motor_command_tmp;
    
    if (!ready_to_start_ || playback_complete_) {
      // Before playback or after completion: maintain current position with low stiffness
      for (int i = 0; i < G1_NUM_MOTOR; ++i) {
        motor_command_tmp.q_target.at(i) = ms->q.at(i);
        motor_command_tmp.dq_target.at(i) = 0.0;
        motor_command_tmp.tau_ff.at(i) = 0.0;
        motor_command_tmp.kp.at(i) = 5.0;  // Low stiffness
        motor_command_tmp.kd.at(i) = GetPlaybackKd(G1MotorType[i]);
      }
    } else {
      // During playback: follow trajectory
      double playback_time = time_ - playback_start_time_;
      TrajectoryPoint target = InterpolateTrajectory(playback_time);
      
      // Set non-arm joints to current position with low stiffness
      for (int i = 0; i < G1_NUM_MOTOR; ++i) {
        motor_command_tmp.q_target.at(i) = ms->q.at(i);
        motor_command_tmp.dq_target.at(i) = 0.0;
        motor_command_tmp.tau_ff.at(i) = 0.0;
        motor_command_tmp.kp.at(i) = 5.0;  // Low stiffness for non-arm joints
        motor_command_tmp.kd.at(i) = GetPlaybackKd(G1MotorType[i]);
      }
      
      // Set arm joints to trajectory targets
      for (int i = 0; i < 14; ++i) {
        int joint_idx = LeftShoulderPitch + i;
        motor_command_tmp.q_target.at(joint_idx) = target.arm_positions[i];
        motor_command_tmp.dq_target.at(joint_idx) = target.arm_velocities[i];
        motor_command_tmp.kp.at(joint_idx) = GetPlaybackKp(G1MotorType[joint_idx]);
        motor_command_tmp.kd.at(joint_idx) = GetPlaybackKd(G1MotorType[joint_idx]);
      }
      
      // Progress reporting
      if (static_cast<int>(playback_time * 10) % 10 == 0) {  // Every 1 second
        std::cout << "Playback progress: " << std::fixed << std::setprecision(1) 
                  << playback_time << "s / " << trajectory_.back().timestamp << "s" 
                  << " (" << (playback_time / trajectory_.back().timestamp * 100) << "%)"
                  << std::endl;
      }
      
      if (playback_complete_) {
        std::cout << "Playback completed!" << std::endl;
      }
    }

    motor_command_buffer_.SetData(motor_command_tmp);
  }

  int queryMotionStatus() {
    std::string robotForm, motionName;
    int32_t ret = msc->CheckMode(robotForm, motionName);
    if (ret == 0) {
      std::cout << "CheckMode succeeded." << std::endl;
    } else {
      std::cout << "CheckMode failed. Error code: " << ret << std::endl;
    }
    if (motionName.empty()) {
      std::cout << "The motion control-related service is deactivated." << std::endl;
      return 0;
    } else {
      std::cout << "Motion service is active: " << motionName << std::endl;
      return 1;
    }
  }
  
  bool IsPlaybackComplete() const {
    return playback_complete_;
  }
};

int main(int argc, char const *argv[]) {
  signal(SIGINT, signalHandler);
  signal(SIGTERM, signalHandler);
  
  if (argc < 3) {
    std::cout << "Usage: g1_arm_playback network_interface_name csv_file_path" << std::endl;
    std::cout << "Example: g1_arm_playback enp3s0 g1_arm_states_20250926_145638.csv" << std::endl;
    std::cout << "This program plays back recorded arm joint trajectories." << std::endl;
    exit(0);
  }
  
  std::string networkInterface = argv[1];
  std::string csv_filename = argv[2];
  
  std::cout << "Starting G1 Arm Playback..." << std::endl;
  
  try {
    G1ArmPlayback playback(networkInterface, csv_filename);
    
    playback.WaitForStart();
    
    while (g_running && !playback.IsPlaybackComplete()) {
      sleep(1);
    }
    
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }
  
  std::cout << "Playback finished." << std::endl;
  return 0;
}
