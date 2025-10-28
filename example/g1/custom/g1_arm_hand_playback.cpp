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
#include <unitree/robot/g1/loco/g1_loco_client.hpp>


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

using namespace unitree::robot::b2;

unitree::robot::ChannelPublisherPtr<unitree_hg::msg::dds_::HandCmd_> handcmd_publisher_left;
unitree::robot::ChannelPublisherPtr<unitree_hg::msg::dds_::HandCmd_> handcmd_publisher_right;

unitree::robot::ChannelSubscriberPtr<unitree_hg::msg::dds_::HandState_> handstate_subscriber_left;
unitree::robot::ChannelSubscriberPtr<unitree_hg::msg::dds_::HandState_> handstate_subscriber_right;

static const std::string HG_CMD_TOPIC = "rt/arm_sdk";
static const std::string HG_STATE_TOPIC = "rt/lowstate";

using namespace unitree::common;
using namespace unitree::robot;

// Global flag for graceful shutdown
volatile bool g_running = true;

void signalHandler(int signum) {
    std::cout << "\nInterrupt signal (" << signum << ") received. Stopping playback..." << std::endl;
    g_running = false;
}

struct TrajectoryPoint {
  double timestamp;
  std::array<float, 14> arm_positions;  // joints 15-28
  std::array<float, 14> arm_velocities;
  std::array<float, 7> hand_positions_left;
  std::array<float, 7> hand_positions_right;
};

class G1ArmPlayback {
 private:
  double time_;
  double control_dt_;  // [2ms]
  double playback_start_time_;
  PRorAB mode_;
  uint8_t mode_machine_;
  
  std::vector<TrajectoryPoint> trajectory_;
  size_t current_frame_index_;
  bool playback_complete_;
  bool first_state_received_;
  bool first_state_received_hand_left_;
  bool first_state_received_hand_right_;

  bool ready_to_start_;
  
  DataBuffer<MotorState> motor_state_buffer_;
  DataBuffer<MotorCommand> motor_command_buffer_;
  DataBuffer<ImuState> imu_state_buffer_;

  DataBuffer<MotorStateHand> motor_state_buffer_hand_left;
  DataBuffer<MotorStateHand> motor_state_buffer_hand_right;
  DataBuffer<MotorCommandHand> hand_command_buffer_left_;
  DataBuffer<MotorCommandHand> hand_command_buffer_right_;

  ChannelPublisherPtr<unitree_hg::msg::dds_::LowCmd_> lowcmd_publisher_;
  ChannelSubscriberPtr<unitree_hg::msg::dds_::LowState_> lowstate_subscriber_;
  ThreadPtr command_writer_ptr_, control_thread_ptr_;

  std::shared_ptr<MotionSwitcherClient> msc;
  std::shared_ptr<unitree::robot::g1::LocoClient> loco_client_;

 public:
  G1ArmPlayback(std::string networkInterface, std::string csv_filename)
      : time_(0.0),
        control_dt_(0.002),
        playback_start_time_(0.0),
        mode_(PR),
        mode_machine_(0),
        current_frame_index_(0),
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

    // Initialize loco client for standing up
    loco_client_.reset(new unitree::robot::g1::LocoClient());
    loco_client_->Init();
    loco_client_->SetTimeout(10.0f);

    // msc.reset(new MotionSwitcherClient());
    // msc->SetTimeout(5.0F);
    // msc->Init();

    // /*Shut down motion control-related service*/
    // while(queryMotionStatus())
    // {
    //     std::cout << "Try to deactivate the motion control-related service." << std::endl;
    //     int32_t ret = msc->ReleaseMode(); 
    //     if (ret == 0) {
    //         std::cout << "ReleaseMode succeeded." << std::endl;
    //     } else {
    //         std::cout << "ReleaseMode failed. Error code: " << ret << std::endl;
    //     }
    //     sleep(5);
    // }

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

    // create hand publisher
    handcmd_publisher_left.reset(new unitree::robot::ChannelPublisher<unitree_hg::msg::dds_::HandCmd_>("rt/dex3/left/cmd"));
    handcmd_publisher_right.reset(new unitree::robot::ChannelPublisher<unitree_hg::msg::dds_::HandCmd_>("rt/dex3/right/cmd"));
    handstate_subscriber_left.reset(new unitree::robot::ChannelSubscriber<unitree_hg::msg::dds_::HandState_>("rt/dex3/left/state"));
    handstate_subscriber_right.reset(new unitree::robot::ChannelSubscriber<unitree_hg::msg::dds_::HandState_>("rt/dex3/right/state"));

    handcmd_publisher_left->InitChannel();
    handcmd_publisher_right->InitChannel();
    handstate_subscriber_left->InitChannel([this](const void *message) {
      this->LowStateHandlerHand(message, true);
    }, 1);
    handstate_subscriber_right->InitChannel([this](const void *message) {
      this->LowStateHandlerHand(message, false);
    }, 1);

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
        } else if (col >= 29 && col <= 35) {
          point.hand_positions_left[col-29] = value;
        } else if (col >= 36 && col <= 42) {
          point.hand_positions_right[col-36] = value;
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

  void StandUpRobot() {
    std::cout << "\n=== Standing up robot ===" << std::endl;
    
    // Get current FSM state
    int fsm_id, fsm_mode;
    loco_client_->GetFsmId(fsm_id);
    loco_client_->GetFsmMode(fsm_mode);
    std::cout << "Current FSM ID: " << fsm_id << ", FSM Mode: " << fsm_mode << std::endl;
    
    // First damp the robot
    std::cout << "Damping robot..." << std::endl;
    int32_t ret = loco_client_->Damp();
    if (ret == 0) {
      std::cout << "Damp command sent successfully" << std::endl;
    } else {
      std::cout << "Damp command failed with error: " << ret << std::endl;
    }
    sleep(5);
    
    // Stand up
    std::cout << "BalanceStand up robot..." << std::endl;
    ret = loco_client_->SetFsmId(4);  // 4 is the FSM ID for standing up
    if (ret == 0) {
      std::cout << "StandUp command sent successfully" << std::endl;
    } else {
      std::cout << "StandUp command failed with error: " << ret << std::endl;
    }
    
    // Check final state
    loco_client_->GetFsmId(fsm_id);
    loco_client_->GetFsmMode(fsm_mode);
    std::cout << "After standing: FSM ID: " << fsm_id << ", FSM Mode: " << fsm_mode << std::endl;
    std::cout << "=== Robot is standing ===" << std::endl << std::endl;

    sleep(10);  // Give time for robot to stand up
    // Start the robot
    std::cout << "Starting robot..." << std::endl;
    ret = loco_client_->Start();
    if (ret == 0) {
      std::cout << "Start command sent successfully" << std::endl;
    } else {
      std::cout << "Start command failed with error: " << ret << std::endl;
    }
    loco_client_->GetFsmId(fsm_id);
    loco_client_->GetFsmMode(fsm_mode);
    std::cout << "After standing: FSM ID: " << fsm_id << ", FSM Mode: " << fsm_mode << std::endl;
    
    sleep(5);
  }

  void WaitForStart() {
    while (!first_state_received_ && g_running) {
      std::cout << "Waiting for robot state..." << std::endl;
      sleep(1);
    }
    
    if (!g_running) return;
    
    std::cout << "Robot state received." << std::endl;
    
    // Stand up the robot
    StandUpRobot();
    
    std::cout << "Press Enter to start arm playback..." << std::endl;
    std::cin.get();
    ready_to_start_ = true;
    playback_start_time_ = time_;
    std::cout << "Starting playback!" << std::endl;
  }

  TrajectoryPoint GetCurrentFrame() {
    // Direct frame access - no interpolation needed since data is at 2ms intervals
    if (current_frame_index_ >= trajectory_.size()) {
      playback_complete_ = true;
      return trajectory_.back();
    }
    
    return trajectory_[current_frame_index_];
  }

  void LowCommandWriter() {
    unitree_hg::msg::dds_::LowCmd_ dds_low_command;
    dds_low_command.mode_pr() = mode_;
    dds_low_command.mode_machine() = mode_machine_;

    const std::shared_ptr<const MotorCommand> mc =
        motor_command_buffer_.GetData();
    if (mc) {
      dds_low_command.motor_cmd().at(29).q(1.0);  // Enable arm SDK control
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

    unitree_hg::msg::dds_::HandCmd_ dds_hand_command_left;
    unitree_hg::msg::dds_::HandCmd_ dds_hand_command_right;
    dds_hand_command_left.motor_cmd().resize(HAND_MOTOR_MAX);
    dds_hand_command_right.motor_cmd().resize(HAND_MOTOR_MAX);
    
    const std::shared_ptr<const MotorCommandHand> mc_left = hand_command_buffer_left_.GetData();
    const std::shared_ptr<const MotorCommandHand> mc_right = hand_command_buffer_right_.GetData();
    if (mc_left) {
      // write hand command
      for (int i = 0; i < HAND_MOTOR_MAX; i++) {
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
    MotorCommandHand hand_command_tmp_left;
    MotorCommandHand hand_command_tmp_right;
    
    if (!ready_to_start_ || playback_complete_) {
      // Before playback or after completion: maintain current position with low stiffness
      for (int i = 0; i < G1_NUM_MOTOR; ++i) {
        motor_command_tmp.q_target.at(i) = ms->q.at(i);
        motor_command_tmp.dq_target.at(i) = 0.0;
        motor_command_tmp.tau_ff.at(i) = 0.0;
        motor_command_tmp.kp.at(i) = 5.0;  // Low stiffness
        motor_command_tmp.kd.at(i) = GetMotorKd(G1MotorType[i]);
      }
      for (int i = 0; i < HAND_MOTOR_MAX; i++) {
        hand_command_tmp_left.q_target.at(i) = ms_left->q.at(i);
        hand_command_tmp_left.dq_target.at(i) = 0.0;
        hand_command_tmp_left.tau_ff.at(i) = 0.0;
        hand_command_tmp_left.kp.at(i) = 1.0;
        hand_command_tmp_left.kd.at(i) = 0.1;
      }
      for (int i = 0; i < HAND_MOTOR_MAX; i++) {
        hand_command_tmp_right.q_target.at(i) = ms_right->q.at(i);
        hand_command_tmp_right.dq_target.at(i) = 0.0;
        hand_command_tmp_right.tau_ff.at(i) = 0.0;
        hand_command_tmp_right.kp.at(i) = 1.0;
        hand_command_tmp_right.kd.at(i) = 0.1;
      }
      } else {
        // During playback: follow trajectory frame by frame
        TrajectoryPoint target = GetCurrentFrame();
        
        // Set all joints to current position with low stiffness
        for (int i = 0; i < G1_NUM_MOTOR; ++i) {
          motor_command_tmp.q_target.at(i) = ms->q.at(i);
          motor_command_tmp.dq_target.at(i) = 0.0;
          motor_command_tmp.tau_ff.at(i) = 0.0;
          motor_command_tmp.kp.at(i) = 5.0;  // Low stiffness for all joints
          motor_command_tmp.kd.at(i) = GetMotorKd(G1MotorType[i]);
        }
        
        // Set arm joints to trajectory targets
        for (int i = 0; i < 14; ++i) {  
          int joint_idx = LeftShoulderPitch + i;
          motor_command_tmp.q_target.at(joint_idx) = target.arm_positions[i];
          motor_command_tmp.dq_target.at(joint_idx) = target.arm_velocities[i];
          motor_command_tmp.kp.at(joint_idx) = GetMotorKp(G1MotorType[joint_idx]);
          motor_command_tmp.kd.at(joint_idx) = GetMotorKd(G1MotorType[joint_idx]);
        }

        for (int i = 0; i < HAND_MOTOR_MAX; i++) {
          hand_command_tmp_left.q_target.at(i) = target.hand_positions_left[i];
          hand_command_tmp_left.dq_target.at(i) = 0.0;
          hand_command_tmp_left.kp.at(i) = 1.0;
          hand_command_tmp_left.kd.at(i) = 0.1;
        }
        
        for (int i = 0; i < HAND_MOTOR_MAX; i++) {
          hand_command_tmp_right.q_target.at(i) = target.hand_positions_right[i];
          hand_command_tmp_right.dq_target.at(i) = 0.0;
          hand_command_tmp_right.kp.at(i) = 1.0;
          hand_command_tmp_right.kd.at(i) = 0.1;
        }
        
        // Advance to next frame
        current_frame_index_++;
        
        // Progress reporting every 250 frames (500ms at 500Hz)
        if (current_frame_index_ % 250 == 0) {
          double progress_percent = (double)current_frame_index_ / trajectory_.size() * 100.0;
          std::cout << "Playback progress: Frame " << current_frame_index_ 
                    << " / " << trajectory_.size() 
                    << " (" << std::fixed << std::setprecision(1) << progress_percent << "%)"
                    << std::endl;
        }
        
        if (playback_complete_) {
          std::cout << "Playback completed!" << std::endl;
        }
      }

    motor_command_buffer_.SetData(motor_command_tmp);
    hand_command_buffer_left_.SetData(hand_command_tmp_left);
    hand_command_buffer_right_.SetData(hand_command_tmp_right);
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
  
  // csv file path
  std::string csv_filename = "/home/jpapagno/projects/unitree_sdk2_fork/build/g1_arm_states_20251023_141546.csv";

  if (argc < 2) {
    std::cout << "Usage: g1_arm_playback network_interface_name" << std::endl;
    std::cout << "Example: g1_arm_playback enp3s0" << std::endl;
    std::cout << "This program plays back recorded arm joint trajectories." << std::endl;
    exit(0);
  }
  
  std::string networkInterface = argv[1];
  
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
