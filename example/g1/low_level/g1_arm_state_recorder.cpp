#include <cmath>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <fstream>
#include <iomanip>
#include <chrono>
#include <sstream>

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

// Low damping values to keep robot compliant but stable
float GetMotorKp(MotorType type) {
  switch (type) {
    case GearboxS:
      return 5;   // Very low stiffness for compliance
    case GearboxM:
      return 5;
    case GearboxL:
      return 10;
    default:
      return 0;
  }
}

float GetMotorKd(MotorType type) {
  switch (type) {
    case GearboxS:
      return 2;   // Higher damping for stability
    case GearboxM:
      return 2;
    case GearboxL:
      return 3;
    default:
      return 0;
  }
}

class G1ArmRecorder {
 private:
  double time_;
  double control_dt_;  // [2ms]
  PRorAB mode_;
  uint8_t mode_machine_;
  
  std::ofstream log_file_;
  int log_counter_;
  bool first_state_received_;

  DataBuffer<MotorState> motor_state_buffer_;
  DataBuffer<MotorCommand> motor_command_buffer_;
  DataBuffer<ImuState> imu_state_buffer_;

  ChannelPublisherPtr<unitree_hg::msg::dds_::LowCmd_> lowcmd_publisher_;
  ChannelSubscriberPtr<unitree_hg::msg::dds_::LowState_> lowstate_subscriber_;
  ThreadPtr command_writer_ptr_, control_thread_ptr_;

  std::shared_ptr<MotionSwitcherClient> msc;

 public:
  G1ArmRecorder(std::string networkInterface)
      : time_(0.0),
        control_dt_(0.002),
        mode_(PR),
        mode_machine_(0),
        log_counter_(0),
        first_state_received_(false) {
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

    // Create log file with timestamp
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << std::put_time(std::localtime(&time_t), "%Y%m%d_%H%M%S");
    
    std::string filename = "g1_arm_states_" + ss.str() + ".csv";
    log_file_.open(filename);
    
    if (log_file_.is_open()) {
      std::cout << "Logging arm states to: " << filename << std::endl;
      
      // Write CSV header
      log_file_ << "timestamp,";
      log_file_ << "left_shoulder_pitch,left_shoulder_roll,left_shoulder_yaw,left_elbow,";
      log_file_ << "left_wrist_roll,left_wrist_pitch,left_wrist_yaw,";
      log_file_ << "right_shoulder_pitch,right_shoulder_roll,right_shoulder_yaw,right_elbow,";
      log_file_ << "right_wrist_roll,right_wrist_pitch,right_wrist_yaw,";
      log_file_ << "left_shoulder_pitch_vel,left_shoulder_roll_vel,left_shoulder_yaw_vel,left_elbow_vel,";
      log_file_ << "left_wrist_roll_vel,left_wrist_pitch_vel,left_wrist_yaw_vel,";
      log_file_ << "right_shoulder_pitch_vel,right_shoulder_roll_vel,right_shoulder_yaw_vel,right_elbow_vel,";
      log_file_ << "right_wrist_roll_vel,right_wrist_pitch_vel,right_wrist_yaw_vel" << std::endl;
    } else {
      std::cerr << "Failed to open log file: " << filename << std::endl;
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
        std::bind(&G1ArmRecorder::LowStateHandler, this, std::placeholders::_1), 1);

    // create threads
    command_writer_ptr_ =
        CreateRecurrentThreadEx("command_writer", UT_CPU_ID_NONE, 2000,
                                &G1ArmRecorder::LowCommandWriter, this);
    control_thread_ptr_ = CreateRecurrentThreadEx(
        "control", UT_CPU_ID_NONE, 2000, &G1ArmRecorder::Control, this);
        
    std::cout << "G1 Arm State Recorder started." << std::endl;
    std::cout << "Robot will be in damped mode (low stiffness, high damping)." << std::endl;
    std::cout << "Press Ctrl+C to stop recording." << std::endl;
  }

  ~G1ArmRecorder() {
    if (log_file_.is_open()) {
      log_file_.close();
      std::cout << "Log file closed." << std::endl;
    }
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

  void LogArmStates() {
    const std::shared_ptr<const MotorState> ms = motor_state_buffer_.GetData();
    if (!ms || !log_file_.is_open()) return;

    // Log every 50ms (25 control cycles at 500Hz)
    if (log_counter_ % 25 == 0) {
      log_file_ << std::fixed << std::setprecision(6) << time_ << ",";
      
      // Log arm joint positions (joints 15-28)
      for (int i = LeftShoulderPitch; i <= RightWristYaw; ++i) {
        log_file_ << ms->q.at(i);
        if (i < RightWristYaw) log_file_ << ",";
      }
      log_file_ << ",";
      
      // Log arm joint velocities (joints 15-28)
      for (int i = LeftShoulderPitch; i <= RightWristYaw; ++i) {
        log_file_ << ms->dq.at(i);
        if (i < RightWristYaw) log_file_ << ",";
      }
      log_file_ << std::endl;
      
      // Console output every 2 seconds
      if (log_counter_ % 1000 == 0) {
        std::cout << "Recording... Time: " << std::fixed << std::setprecision(2) 
                  << time_ << "s" << std::endl;
      }
    }
    log_counter_++;
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
    
    // Set all joints to zero stiffness for manual manipulation
    for (int i = 0; i < G1_NUM_MOTOR; ++i) {
      motor_command_tmp.q_target.at(i) = ms->q.at(i);  // Track current position
      motor_command_tmp.dq_target.at(i) = 0.0;         // No velocity target
      motor_command_tmp.tau_ff.at(i) = 0.0;            // No feedforward torque
      motor_command_tmp.kp.at(i) = 0.0;                // ZERO stiffness - completely compliant
      motor_command_tmp.kd.at(i) = GetMotorKd(G1MotorType[i]);  // Small damping for stability
    }

    motor_command_buffer_.SetData(motor_command_tmp);
    
    // Log arm states
    LogArmStates();
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
};

int main(int argc, char const *argv[]) {
  if (argc < 2) {
    std::cout << "Usage: g1_arm_state_recorder network_interface_name" << std::endl;
    std::cout << "This program records arm joint states while keeping the robot in damped mode." << std::endl;
    exit(0);
  }
  
  std::string networkInterface = argv[1];
  std::cout << "Starting G1 Arm State Recorder..." << std::endl;
  
  G1ArmRecorder recorder(networkInterface);

  while (true) sleep(10);

  return 0;
}
