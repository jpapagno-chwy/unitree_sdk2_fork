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

// constants for the number of motors
const int HAND_MOTOR_MAX = 7;
const int G1_NUM_MOTOR = 29;


// motor state and command for hand
struct MotorStateHand {
    std::array<float, HAND_MOTOR_MAX> q = {};
    std::array<float, HAND_MOTOR_MAX> dq = {};
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

struct MotorCommandHand {
  std::array<float, HAND_MOTOR_MAX> q_target = {};
  std::array<float, HAND_MOTOR_MAX> dq_target = {};
  std::array<float, HAND_MOTOR_MAX> kp = {};
  std::array<float, HAND_MOTOR_MAX> kd = {};
  std::array<float, HAND_MOTOR_MAX> tau_ff = {};
};

typedef struct {
    uint8_t id     : 4;
    uint8_t status : 3;
    uint8_t timeout: 1;
  } RIS_Mode_t;


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
float GetMotorKp(MotorType type) {
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

float GetMotorKd(MotorType type) {
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