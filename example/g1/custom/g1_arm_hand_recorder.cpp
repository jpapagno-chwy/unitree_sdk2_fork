#include <cmath>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <fstream>
#include <iomanip>
#include <chrono>
#include <sstream>
#include <signal.h>

// DDS
#include <unitree/robot/channel/channel_publisher.hpp>
#include <unitree/robot/channel/channel_subscriber.hpp>

// IDL
#include <unitree/idl/hg/LowCmd_.hpp>
#include <unitree/idl/hg/LowState_.hpp>

#include <unitree/robot/b2/motion_switcher/motion_switcher_client.hpp>
#include <chrono>
#include <thread>
#include <unitree/idl/hg/HandState_.hpp> //replace your sdk path
#include <unitree/idl/hg/HandCmd_.hpp> //replace your sdk path
#include <unitree/robot/channel/channel_publisher.hpp>
#include <unitree/robot/channel/channel_subscriber.hpp>
#include <iostream>
#include <unistd.h>
#include <atomic>
#include <mutex>
#include <cmath>
#include <termios.h>
#include <unistd.h>
#include <eigen3/Eigen/Dense>

#include "g1_recorder_structs.h"

// hand dds
unitree::robot::ChannelPublisherPtr<unitree_hg::msg::dds_::HandCmd_> handcmd_publisher_left;
unitree::robot::ChannelPublisherPtr<unitree_hg::msg::dds_::HandCmd_> handcmd_publisher_right;

unitree::robot::ChannelSubscriberPtr<unitree_hg::msg::dds_::HandState_> handstate_subscriber_left;
unitree::robot::ChannelSubscriberPtr<unitree_hg::msg::dds_::HandState_> handstate_subscriber_right;

using namespace unitree::robot::b2;

// Global flag for graceful shutdown
volatile bool g_running = true;

void signalHandler(int signum) {
    std::cout << "\n\n*** INTERRUPT SIGNAL RECEIVED ***" << std::endl;
    std::cout << "Stopping recording and saving data..." << std::endl;
    g_running = false;
}

// Using "rt/lowcmd" for full control (alternative: "rt/arm_sdk" for weight-based control)
static const std::string HG_CMD_TOPIC = "rt/arm_sdk";
static const std::string HG_STATE_TOPIC = "rt/lowstate";

using namespace unitree::common;
using namespace unitree::robot;

class G1ArmRecorder {
 private:
  double time_;
  double control_dt_;  // [2ms]
  PRorAB mode_;
  uint8_t mode_machine_;
  
  std::ofstream log_file_;

  int log_counter_;
  bool first_state_received_;
  bool first_state_received_hand_left_;
  bool first_state_received_hand_right_;
  bool recording_active_;  // Flag to control when to start recording

  DataBuffer<MotorState> motor_state_buffer_;
  DataBuffer<MotorStateHand> motor_state_buffer_hand_left;
  DataBuffer<MotorStateHand> motor_state_buffer_hand_right;
  DataBuffer<MotorCommand> motor_command_buffer_;
  DataBuffer<MotorCommandHand> hand_command_buffer_left_;
  DataBuffer<MotorCommandHand> hand_command_buffer_right_;
  DataBuffer<ImuState> imu_state_buffer_;
  unitree_hg::msg::dds_::HandCmd_ dds_hand_command_left;
  unitree_hg::msg::dds_::HandCmd_ dds_hand_command_right;

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
        first_state_received_(false),
        recording_active_(false) {
    ChannelFactory::Instance()->Init(0, networkInterface);

    msc.reset(new MotionSwitcherClient());
    msc->SetTimeout(5.0F);
    msc->Init();

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
      log_file_ << "waist_yaw,waist_roll,waist_pitch,";
      log_file_ << "left_shoulder_pitch,left_shoulder_roll,left_shoulder_yaw,left_elbow,";
      log_file_ << "left_wrist_roll,left_wrist_pitch,left_wrist_yaw,";
      log_file_ << "right_shoulder_pitch,right_shoulder_roll,right_shoulder_yaw,right_elbow,";
      log_file_ << "right_wrist_roll,right_wrist_pitch,right_wrist_yaw,";
      log_file_ << "waist_yaw_vel,waist_roll_vel,waist_pitch_vel,";
      log_file_ << "left_shoulder_pitch_vel,left_shoulder_roll_vel,left_shoulder_yaw_vel,left_elbow_vel,";
      log_file_ << "left_wrist_roll_vel,left_wrist_pitch_vel,left_wrist_yaw_vel,";
      log_file_ << "right_shoulder_pitch_vel,right_shoulder_roll_vel,right_shoulder_yaw_vel,right_elbow_vel,";
      log_file_ << "right_wrist_roll_vel,right_wrist_pitch_vel,right_wrist_yaw_vel,";
      log_file_ << "left_thumb_rot, left_thumb_prox, left_thumb_dist, left_middle_prox, left_middle_dist, left_index_prox, left_index_dist,";
      log_file_ << "right_thumb_rot, right_thumb_prox, right_thumb_dist, right_middle_prox, right_middle_dist, right_index_prox, right_index_dist" << std::endl;
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

    //   dex3 hand subscribers and publishers
    handcmd_publisher_left.reset(new unitree::robot::ChannelPublisher<unitree_hg::msg::dds_::HandCmd_>("rt/dex3/left/cmd"));
    handcmd_publisher_right.reset(new unitree::robot::ChannelPublisher<unitree_hg::msg::dds_::HandCmd_>("rt/dex3/right/cmd"));
    handstate_subscriber_left.reset(new unitree::robot::ChannelSubscriber<unitree_hg::msg::dds_::HandState_>("rt/lf/dex3/left/state"));
    handstate_subscriber_right.reset(new unitree::robot::ChannelSubscriber<unitree_hg::msg::dds_::HandState_>("rt/lf/dex3/right/state"));
    handcmd_publisher_left->InitChannel();
    handcmd_publisher_right->InitChannel();
    handstate_subscriber_left->InitChannel([this](const void *message) {
      this->LowStateHandlerHand(message, true);
    }, 1);
    handstate_subscriber_right->InitChannel([this](const void *message) {
      this->LowStateHandlerHand(message, false);
    }, 1);
        
    std::cout << "G1 Arm State Recorder initialized." << std::endl;
    std::cout << "Robot will be in damped mode (low stiffness, high damping)." << std::endl;
  }
  
  void WaitForRecordingStart() {
    // Wait for robot state to be received
    while (!first_state_received_ && g_running) {
      std::cout << "Waiting for robot state..." << std::endl;
      sleep(1);
    }
    
    if (!g_running) return;
    
    std::cout << "\n=== Ready to Record ===" << std::endl;
    std::cout << "Robot is in damped mode - you can manually move the arms." << std::endl;
    std::cout << "Position the robot as desired, then press ENTER to start recording..." << std::endl;
    std::cin.get();
    
    if (!g_running) return;
    
    // Countdown
    std::cout << "\nStarting recording in:" << std::endl;
    for (int i = 5; i > 0; i--) {
      std::cout << "  " << i << "..." << std::endl;
      sleep(1);
      if (!g_running) return;
    }
    
    std::cout << "\n*** RECORDING STARTED ***" << std::endl;
    std::cout << "Recording is now active - move the robot as desired." << std::endl;
    std::cout << "Press Ctrl+C to stop recording when finished." << std::endl << std::endl;
    recording_active_ = true;
  }

  ~G1ArmRecorder() {
    Shutdown();
  }

  void Shutdown() {
    std::cout << "Shutting down G1 Arm Recorder..." << std::endl;
    
    // Stop control threads gracefully
    if (control_thread_ptr_) {
      control_thread_ptr_.reset();
      std::cout << "Control thread stopped." << std::endl;
    }
    
    if (command_writer_ptr_) {
      command_writer_ptr_.reset();
      std::cout << "Command writer thread stopped." << std::endl;
    }
    
    // Send final damped command to ensure robot stays safe
    SendFinalDampedCommand();
    
    // Close log file
    if (log_file_.is_open()) {
      log_file_.close();
      std::cout << "Log file closed. Total records: " << log_counter_ << std::endl;
    }
    
    std::cout << "Shutdown complete." << std::endl;
  }

private:
  void SendFinalDampedCommand() {
    // Send one final command to ensure robot stays in damped mode
    const std::shared_ptr<const MotorState> ms = motor_state_buffer_.GetData();
    if (!ms) return;
    
    unitree_hg::msg::dds_::LowCmd_ dds_low_command;
    dds_low_command.mode_pr() = mode_;
    dds_low_command.mode_machine() = mode_machine_;
    
    // Set all motors to damped mode (zero stiffness, small damping)
    for (size_t i = 0; i < G1_NUM_MOTOR; i++) {
      dds_low_command.motor_cmd().at(i).mode() = 1;  // Enable
      dds_low_command.motor_cmd().at(i).tau() = 0.0;  // No feedforward torque
      dds_low_command.motor_cmd().at(i).q() = ms->q.at(i);  // Current position
      dds_low_command.motor_cmd().at(i).dq() = 0.0;  // No velocity target
      dds_low_command.motor_cmd().at(i).kp() = 0.0;  // Zero stiffness
      dds_low_command.motor_cmd().at(i).kd() = GetMotorKd(G1MotorType[i]);  // Small damping
    }
    
    dds_low_command.crc() = Crc32Core((uint32_t *)&dds_low_command,
                                      (sizeof(dds_low_command) >> 2) - 1);
    
    if (lowcmd_publisher_) {
      lowcmd_publisher_->Write(dds_low_command);
      std::cout << "Final damped command sent to robot." << std::endl;
    }
  }

public:

  void LowStateHandlerHand(const void *message, bool is_left) {
    auto hand_state = *(const unitree_hg::msg::dds_::HandState_ *)message;

    // get motor state
    MotorStateHand hand_ms_tmp;
    for (int i = 0; i < HAND_MOTOR_MAX; ++i) {
      hand_ms_tmp.q.at(i) = hand_state.motor_state()[i].q();
      hand_ms_tmp.dq.at(i) = hand_state.motor_state()[i].dq();
    }
    
    if (is_left) {
      motor_state_buffer_hand_left.SetData(hand_ms_tmp);
      first_state_received_hand_left_ = true;
    } else {
      motor_state_buffer_hand_right.SetData(hand_ms_tmp);
      first_state_received_hand_right_ = true;
    }
    
  }

  void LowStateHandler(const void *message) {
    auto low_state = *(const unitree_hg::msg::dds_::LowState_ *)message;

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
    // Only log if recording is active
    if (!recording_active_) return;
    
    const std::shared_ptr<const MotorState> ms = motor_state_buffer_.GetData();
    const std::shared_ptr<const MotorStateHand> ms_left = motor_state_buffer_hand_left.GetData();
    const std::shared_ptr<const MotorStateHand> ms_right = motor_state_buffer_hand_right.GetData();
    if (!ms || !ms_left || !ms_right || !log_file_.is_open()) return;

    // Log every 2ms (every control cycle at 500Hz)
    log_file_ << std::fixed << std::setprecision(6) << time_ << ",";
    
    // Log waist joint 14 position
    log_file_ << ms->q.at(12) << ",";
    log_file_ << ms->q.at(13) << ",";
    log_file_ << ms->q.at(14) << ",";
    
    // Log arm joint positions (joints 15-28)
    for (int i = LeftShoulderPitch; i <= RightWristYaw; ++i) {
      log_file_ << ms->q.at(i);
      if (i < RightWristYaw) log_file_ << ",";
    }
    log_file_ << ",";
    
    // Log waist joint 14 velocity
    log_file_ << ms->dq.at(12) << ",";
    log_file_ << ms->dq.at(13) << ",";
    log_file_ << ms->dq.at(14) << ",";
    
    // Log arm joint velocities (joints 15-28)
    for (int i = LeftShoulderPitch; i <= RightWristYaw; ++i) {
      log_file_ << ms->dq.at(i);
      if (i < RightWristYaw) log_file_ << ",";
    }

    log_file_ << ",";
    for (int i = 0; i < HAND_MOTOR_MAX; ++i) {
      log_file_ << ms_left->q.at(i);
      if (i < HAND_MOTOR_MAX - 1) log_file_ << ",";
    }
    log_file_ << ",";
    for (int i = 0; i < HAND_MOTOR_MAX; ++i) {
      log_file_ << ms_right->q.at(i);
      if (i < HAND_MOTOR_MAX - 1) log_file_ << ",";
    }

    log_file_ << std::endl;
  

    // Console output every 2 seconds (1000 control cycles at 500Hz)
    if (log_counter_ % 1000 == 0) {
      std::cout << "Recording... Time: " << std::fixed << std::setprecision(2) 
                << time_ << "s | Records: " << log_counter_ << " | Press Ctrl+C to stop" << std::endl;
    }
    
    log_counter_++;
  }

  void LowCommandWriter() {
    unitree_hg::msg::dds_::LowCmd_ dds_low_command;  //type: LowCmd_ variable name dds_low_command
    dds_low_command.mode_pr() = mode_;  //set the mode_pr field of the dds_low_command to the value of the mode_ variable
    dds_low_command.mode_machine() = mode_machine_;  //set the mode_machine field of the dds_low_command to the value of the mode_machine_ variable
    
    const std::shared_ptr<const MotorCommand> mc =
        motor_command_buffer_.GetData();
    if (mc) {
      dds_low_command.motor_cmd().at(29).q(1.0);  // Enable arm SDK control
      for (size_t i = 0; i < G1_NUM_MOTOR; i++) {  //loop through the motors
        dds_low_command.motor_cmd().at(i).mode() = 1;  // 1:Enable, 0:Disable
        dds_low_command.motor_cmd().at(i).tau() = mc->tau_ff.at(i);  //set the tau_ff field of the dds_low_command to the value of the tau_ff field of the mc variable
        dds_low_command.motor_cmd().at(i).q() = mc->q_target.at(i);  //set the q field of the dds_low_command to the value of the q field of the mc variable
        dds_low_command.motor_cmd().at(i).dq() = mc->dq_target.at(i);  //set the dq field of the dds_low_command to the value of the dq field of the mc variable
        dds_low_command.motor_cmd().at(i).kp() = mc->kp.at(i);  //set the kp field of the dds_low_command to the value of the kp field of the mc variable
        dds_low_command.motor_cmd().at(i).kd() = mc->kd.at(i);  //set the kd field of the dds_low_command to the value of the kd field of the mc variable
      }

      dds_low_command.crc() = Crc32Core((uint32_t *)&dds_low_command,
                                        (sizeof(dds_low_command) >> 2) - 1);
      lowcmd_publisher_->Write(dds_low_command);  //write the dds_low_command to the lowcmd_publisher
    }

    dds_hand_command_left.motor_cmd().resize(HAND_MOTOR_MAX);
    dds_hand_command_right.motor_cmd().resize(HAND_MOTOR_MAX);
    
    const std::shared_ptr<const MotorCommandHand> mc_left = hand_command_buffer_left_.GetData();
    const std::shared_ptr<const MotorCommandHand> mc_right = hand_command_buffer_right_.GetData();
    if (mc_left) {
      // write hand command
      for (int i = 0; i < HAND_MOTOR_MAX; i++) {    // boilerplate to talk to the hand motors
          RIS_Mode_t ris_mode;
          ris_mode.id = i;        
          ris_mode.status = 0x01; 
          uint8_t mode = 0;
          mode |= (ris_mode.id & 0x0F);            
          mode |= (ris_mode.status & 0x07) << 4;    
          mode |= (ris_mode.timeout & 0x01) << 7;  

          dds_hand_command_left.motor_cmd()[i].mode(mode);
          dds_hand_command_left.motor_cmd()[i].tau(mc_left->tau_ff.at(i));

          dds_hand_command_left.motor_cmd()[i].q(mc_left->q_target.at(i)); 
          dds_hand_command_left.motor_cmd()[i].dq(mc_left->dq_target.at(i));  
          dds_hand_command_left.motor_cmd()[i].kp(mc_left->kp.at(i));   
          dds_hand_command_left.motor_cmd()[i].kd(mc_left->kd.at(i));   
      }
      // dds_hand_command_left.crc() = Crc32Core((uint32_t *)&dds_hand_command_left,
      // (sizeof(dds_hand_command_left) >> 2) - 1);
      handcmd_publisher_left->Write(dds_hand_command_left);
    }

    if (mc_right) {
      // write hand command
      for (int i = 0; i < HAND_MOTOR_MAX; i++) {
          RIS_Mode_t ris_mode;
          ris_mode.id = i;      
          ris_mode.status = 0x01; 
          uint8_t mode = 0;
          mode |= (ris_mode.id & 0x0F);            
          mode |= (ris_mode.status & 0x07) << 4;    
          mode |= (ris_mode.timeout & 0x01) << 7;  

          dds_hand_command_right.motor_cmd()[i].mode(mode);
          dds_hand_command_right.motor_cmd()[i].tau(mc_right->tau_ff.at(i));

          dds_hand_command_right.motor_cmd()[i].q(mc_right->q_target.at(i)); 
          dds_hand_command_right.motor_cmd()[i].dq(mc_right->dq_target.at(i));  
          dds_hand_command_right.motor_cmd()[i].kp(mc_right->kp.at(i));   
          dds_hand_command_right.motor_cmd()[i].kd(mc_right->kd.at(i));   
      }
      // dds_hand_command_right.crc() = Crc32Core((uint32_t *)&dds_hand_command_right,
      // (sizeof(dds_hand_command_right) >> 2) - 1);
      handcmd_publisher_right->Write(dds_hand_command_right);
    }
  }

  void Control() {
    const std::shared_ptr<const MotorState> ms = motor_state_buffer_.GetData();
    const std::shared_ptr<const MotorStateHand> ms_left = motor_state_buffer_hand_left.GetData();
    const std::shared_ptr<const MotorStateHand> ms_right = motor_state_buffer_hand_right.GetData();
    if (!ms || !first_state_received_ || !ms_left || !ms_right) return;

    time_ += control_dt_;
    
    MotorCommand motor_command_tmp;
    

    for (int i = 0; i < G1_NUM_MOTOR; ++i) {
      motor_command_tmp.q_target.at(i) = ms->q.at(i);  // Track current position
      motor_command_tmp.dq_target.at(i) = 0.0;         // No velocity target
      motor_command_tmp.tau_ff.at(i) = 0.0;            // No feedforward torque
      motor_command_tmp.kp.at(i) = 0.0;                // ZERO stiffness - completely compliant
      motor_command_tmp.kd.at(i) = GetMotorKd(G1MotorType[i]);  // Small damping for stability
  }

    motor_command_buffer_.SetData(motor_command_tmp);

    // Set hand joints to zero stiffness for manual manipulation
    MotorCommandHand hand_command_tmp;
    for (int i = 0; i < HAND_MOTOR_MAX; i++) {
      hand_command_tmp.q_target.at(i) = ms_left->q.at(i);  // Track current position
      hand_command_tmp.dq_target.at(i) = 0.0;         // No velocity target
      hand_command_tmp.tau_ff.at(i) = 0.0;            // No feedforward torque
      hand_command_tmp.kp.at(i) = 0.0;                // ZERO stiffness - completely compliant
      hand_command_tmp.kd.at(i) = .01;  // Small damping for stability
    }

    hand_command_buffer_left_.SetData(hand_command_tmp);

    for (int i = 0; i < HAND_MOTOR_MAX; i++) {
      hand_command_tmp.q_target.at(i) = ms_right->q.at(i);  // Track current position
      hand_command_tmp.dq_target.at(i) = 0.0;         // No velocity target
      hand_command_tmp.tau_ff.at(i) = 0.0;            // No feedforward torque
      hand_command_tmp.kp.at(i) = 0.0;                // ZERO stiffness - completely compliant
      hand_command_tmp.kd.at(i) = .01;  // Small damping for stability
    }

    hand_command_buffer_right_.SetData(hand_command_tmp);

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
  // Set up signal handlers for graceful shutdown
  signal(SIGINT, signalHandler);
  signal(SIGTERM, signalHandler);
  
  if (argc < 2) {
    std::cout << "Usage: g1_arm_state_recorder network_interface_name" << std::endl;
    std::cout << "This program records arm joint states while keeping the robot in damped mode." << std::endl;
    exit(0);
  }
  
  std::string networkInterface = argv[1];

  std::cout << "Starting G1 Arm State Recorder..." << std::endl;
  
  try {
    G1ArmRecorder recorder(networkInterface);
    
    // Wait for user to be ready and countdown
    recorder.WaitForRecordingStart();

    // Main loop - check for shutdown signal
    while (g_running) {
      sleep(1);  // Check every second instead of sleeping for 10 seconds
    }
    
    // Explicit shutdown call to ensure cleanup happens before destructor
    recorder.Shutdown();
    
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }

  std::cout << "Recording stopped." << std::endl;
  return 0;
}
