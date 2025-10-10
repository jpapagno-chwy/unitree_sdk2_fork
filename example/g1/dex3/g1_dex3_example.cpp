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


enum State {
    INIT,
    ROTATE,
    GRIP,
    STOP,
    PRINT,
    OPEN,
    CLOSE
};

float kp = 0.5;
float kd = 0.01;
float next_kp = 0.5;
float next_kd = 0.01;

// set URDF Limits
const float maxLimits_left[7]=  {  1.05 ,  1.05  , 1.75 ,   0   ,  0    , 0     , 0   }; // set max motor value
const float minLimits_left[7]=  { -1.05 , -0.724 ,   0  , -1.57 , -1.75 , -1.57  ,-1.75}; 
const float maxLimits_right[7]= {  1.05 , 0.742  ,   0  ,  1.57 , 1.75  , 1.57  , 1.75}; 
const float minLimits_right[7]= { -1.05 , -1.05  , -1.75,    0  ,  0    ,   0   ,0    }; 

// Initing the dds configuration
std::string dds_namespace = "rt/dex3/left";
std::string sub_namespace = "rt/dex3/left/state";
unitree::robot::ChannelPublisherPtr<unitree_hg::msg::dds_::HandCmd_> handcmd_publisher;
unitree::robot::ChannelSubscriberPtr<unitree_hg::msg::dds_::HandState_> handstate_subscriber;
unitree_hg::msg::dds_::HandCmd_ msg;
unitree_hg::msg::dds_::HandState_ state;
std::atomic<State> currentState(INIT);
std::mutex stateMutex;

#define MOTOR_MAX 7
#define SENSOR_MAX 9
uint8_t hand_id = 0;

typedef struct {
    uint8_t id     : 4;
    uint8_t status : 3;
    uint8_t timeout: 1;
} RIS_Mode_t;

// stateToString Method
const char* stateToString(State state) {
    switch (state) {
        case INIT: return "INIT";
        case ROTATE: return "ROTATE";
        case GRIP: return "GRIP";
        case STOP: return "STOP";
        case PRINT: return "PRINT";
        case OPEN: return "OPEN";
        case CLOSE: return "CLOSE";
        default: return "UNKNOWN";
    }
}

// Monitor user's input
char getNonBlockingInput() {
    struct termios oldt, newt;
    char ch;
    int oldf;

    tcgetattr(STDIN_FILENO, &oldt); 
    newt = oldt;
    newt.c_lflag &= ~(ICANON | ECHO);
    tcsetattr(STDIN_FILENO, TCSANOW, &newt);
    oldf = fcntl(STDIN_FILENO, F_GETFL, 0);
    fcntl(STDIN_FILENO, F_SETFL, oldf | O_NONBLOCK);

    ch = getchar(); 

    tcsetattr(STDIN_FILENO, TCSANOW, &oldt); 
    fcntl(STDIN_FILENO, F_SETFL, oldf);

    return ch;
}

void userInputThread() {
    while (true) {
        char ch = getNonBlockingInput();
        if (ch == 'q') {
            std::cout << "Exiting..." << std::endl;
                currentState = STOP;
                break;
        } else if (ch == 'r') {
            currentState = ROTATE;
        } else if (ch == 'g') {
            currentState = GRIP;
        } else if (ch == 'p') {
            currentState = PRINT;
        } else if (ch == 's') {
            currentState = STOP;
        } else if (ch == 'o') {
            currentState = OPEN;
        } else if (ch == 'c') {
            currentState = CLOSE;
        } else if (ch == '0') {
            next_kp = kp + 0.02;
        } else if (ch == '9') {
            next_kp = kp - 0.02;
            if (next_kp < 0) {
                next_kp = 0;
            }
        } else if (ch == '+') {
            next_kd = kd + 0.01;
        } else if (ch == '-') {
            next_kd = kd - 0.01;
            if (next_kd < 0) {
                next_kd = 0;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); 
    }
}


 
// this method can send kp and kd to motors
void rotateMotors(bool isLeftHand) {
    static int _count = 1; 
    static int dir = 1;    
    const float* maxLimits = isLeftHand ? maxLimits_left : maxLimits_right;
    const float* minLimits = isLeftHand ? minLimits_left : minLimits_right;

    for (int i = 0; i < MOTOR_MAX; i++) {
        RIS_Mode_t ris_mode;
        ris_mode.id = i;        
        ris_mode.status = 0x01; 
   
        
        uint8_t mode = 0;
        mode |= (ris_mode.id & 0x0F);           
        mode |= (ris_mode.status & 0x07) << 4;    
        mode |= (ris_mode.timeout & 0x01) << 7;  
        msg.motor_cmd()[i].mode(mode);
        msg.motor_cmd()[i].tau(0);
        
        // stiffness
        msg.motor_cmd()[i].kp(0.5);
        // damping 
        msg.motor_cmd()[i].kd(0.1);    


        float range = maxLimits[i] - minLimits[i];
        float mid = (maxLimits[i] + minLimits[i]) / 2.0; 
        float amplitude = range / 2.0; 
        float q = mid + amplitude * sin(_count / 20000.0 * M_PI); 

        msg.motor_cmd()[i].q(q);
    }

    handcmd_publisher->Write(msg);
    _count += dir;


    if (_count >= 10000) {
        dir = -1;
    }
    if (_count <= -10000) {
        dir = 1;
    }

    usleep(100); 
}

// this method can send static position to motors
void moveHand(bool isLeftHand, bool isOpen) {

    const float* maxLimits = isLeftHand ? maxLimits_left : maxLimits_right;
    const float* minLimits = isLeftHand ? minLimits_left : minLimits_right;
    // open  L:   -0.10294  -0.913251 -0.0570956 -0.0390129 -0.0127923 -0.0529036 -0.0269407
    // close  L: -0.0321972   0.658828     1.5053   -1.61362   -1.77055    -1.6244   -1.78361
    // open R: -0.0290204   0.679469 -0.0411741 -0.0671038 -0.0410292 -0.0381968  -0.060765
    // close  R: -0.0300349  -0.930099   -1.57439    1.51264    1.72646    1.53592    1.71186

    std::array<float, 7> right_open_state = {-0.0290204,0.679469,-0.0411741,-0.0671038,-0.0410292,-0.0381968,-0.060765};
    std::array<float, 7> left_open_state = {-0.10294,-0.913251,-0.0570956,-0.0390129,-0.0127923,-0.0529036,-0.0269407};

    std::array<float, 7> right_close_state = {-0.0300349,-0.930099,-0.157439,1.51264,1.72646,1.53592,1.71186};
    std::array<float, 7> left_close_state = {-0.0321972,0.658828,1.5053,-1.61362,-1.77055,-1.6244,-1.78361};

    std::array<float, 7> write_values;
    if (isOpen) {
        write_values = isLeftHand ? left_open_state : right_open_state;
    } else {
        write_values = isLeftHand ? left_close_state : right_close_state;
    }


    for (int i = 0; i < MOTOR_MAX; i++) {
        RIS_Mode_t ris_mode;
        ris_mode.id = i;        
        ris_mode.status = 0x01; 
    
        
        uint8_t mode = 0;
        mode |= (ris_mode.id & 0x0F);            
        mode |= (ris_mode.status & 0x07) << 4;    
        mode |= (ris_mode.timeout & 0x01) << 7;   
        msg.motor_cmd()[i].mode(mode);
        msg.motor_cmd()[i].tau(0);

        msg.motor_cmd()[i].q(write_values[i]); 
        msg.motor_cmd()[i].dq(0);  
        msg.motor_cmd()[i].kp(kp);   
        msg.motor_cmd()[i].kd(kd);   
    }


    handcmd_publisher->Write(msg);
    usleep(1000000);
}

// this method can send static position to motors
void gripHand(bool isLeftHand) {

    const float* maxLimits = isLeftHand ? maxLimits_left : maxLimits_right;
    const float* minLimits = isLeftHand ? minLimits_left : minLimits_right;

    for (int i = 0; i < MOTOR_MAX; i++) {
        RIS_Mode_t ris_mode;
        ris_mode.id = i;        
        ris_mode.status = 0x01; 
    
        
        uint8_t mode = 0;
        mode |= (ris_mode.id & 0x0F);            
        mode |= (ris_mode.status & 0x07) << 4;    
        mode |= (ris_mode.timeout & 0x01) << 7;   
        msg.motor_cmd()[i].mode(mode);
        msg.motor_cmd()[i].tau(0);

      
        float mid = (maxLimits[i] + minLimits[i]) / 2.0;


        msg.motor_cmd()[i].q(mid); 
        msg.motor_cmd()[i].dq(0);  
        msg.motor_cmd()[i].kp(1.5);      
        msg.motor_cmd()[i].kd(0.1);   
    }


    handcmd_publisher->Write(msg);
    usleep(1000000);
}

// this method can send dynamic position to motors
void stopMotors() {
    for (int i = 0; i < MOTOR_MAX; i++) {
        RIS_Mode_t ris_mode;
        ris_mode.id = i;       
        ris_mode.status = 0x01; 
        ris_mode.timeout = 0x01; 
        
        uint8_t mode = 0;
        mode |= (ris_mode.id & 0x0F);            
        mode |= (ris_mode.status & 0x07) << 4;  
        mode |= (ris_mode.timeout & 0x01) << 7;   
        msg.motor_cmd()[i].mode(mode);
        msg.motor_cmd()[i].tau(0);
        msg.motor_cmd()[i].dq(0); 
        msg.motor_cmd()[i].kp(0);
        msg.motor_cmd()[i].kd(0);
        msg.motor_cmd()[i].q(0); 

    }
    handcmd_publisher->Write(msg);
    usleep(1000000); 
}

// this method can subscribe dds and show the position for now
void printState(bool isLeftHand, State cur_state){
    Eigen::Matrix<float, 7, 1> q;

    const float* maxLimits = isLeftHand ? maxLimits_left : maxLimits_right;
    const float* minLimits = isLeftHand ? minLimits_left : minLimits_right;
    for(int i = 0; i < 7; i++) 
    {
        q(i) = state.motor_state()[i].q();
      
        // q(i) = (q(i) - minLimits[i] ) / (maxLimits[i] - minLimits[i]);
        // q(i) = std::clamp(q(i), 0.0f, 1.0f);
    }
    std::cout << "\033[2J\033[H"; 
    std::cout << "-- Hand State --\n";
    std::cout << "--- Current State: " << "Test" << " ---\n";
    std::cout << "Commands:\n";
    std::cout << "  r - Rotate\n";
    std::cout << "  g - Grip\n";
    std::cout << "  t - Test\n";
    std::cout << "  q - Quit\n";
    if(isLeftHand){
        std::cout << " L: " << q.transpose() << std::endl;
    }else std::cout << " R: " << q.transpose() << std::endl;
    usleep(0.1 * 1e6);

}

void StateHandler(const void *message) {
  state = *(unitree_hg::msg::dds_::HandState_ *)message;
}




int main(int argc, const char** argv)
{
    std::cout << " --- Unitree Robotics --- \n";
    std::cout << "     Dex3 Hand Example      \n\n";
    std::string input;
    std::cout << "Please input the hand id (L for left hand, R for right hand): ";
    std::cin >> input;

    if (input == "L") {
        hand_id = 0;
        dds_namespace = "rt/dex3/left";
        sub_namespace = "rt/lf/dex3/left/state";
    } else if (input == "R") {
        hand_id = 1;
        dds_namespace = "rt/dex3/right";
        sub_namespace = "rt/lf/dex3/right/state";
    } else {
        std::cout << "Invalid hand id. Please input 'L' or 'R'." << std::endl;
        return -1;
    }

    if (argc < 2)
    {
        std::cout << "Usage: " << argv[0] << " networkInterface" << std::endl;
        exit(-1); 
    }
    unitree::robot::ChannelFactory::Instance()->Init(0, argv[1]);
    handcmd_publisher.reset(new unitree::robot::ChannelPublisher<unitree_hg::msg::dds_::HandCmd_>(dds_namespace + "/cmd"));
    handstate_subscriber.reset(new unitree::robot::ChannelSubscriber<unitree_hg::msg::dds_::HandState_>(sub_namespace));
    handcmd_publisher->InitChannel();
    handstate_subscriber->InitChannel(
      std::bind(&StateHandler, std::placeholders::_1), 1);
    state.motor_state().resize(MOTOR_MAX);
    state.press_sensor_state().resize(SENSOR_MAX);
    msg.motor_cmd().resize(MOTOR_MAX);
    
    // handcmd_publisher->msg_.motor_cmd().resize(MOTOR_MAX);

   
    std::thread inputThread(userInputThread);
    State lastState = INIT; 
    bool stiffness_changed = false;
    while (true) {
        State state;
        {
            std::lock_guard<std::mutex> lock(stateMutex);
            state = currentState.load();
            if (next_kp != kp || next_kd != kd) {
                kp = next_kp;
                kd = next_kd;
                // print the current kp and kd
                std::cout << "kp: " << kp << " kd: " << kd << std::endl;
                stiffness_changed = true;
            }
        }
                
        if (state != lastState || stiffness_changed) {
            std::cout << "\n--- Current State: " << stateToString(state) << " ---\n";
            std::cout << "Commands:\n";
            std::cout << "  r - Rotate\n";
            std::cout << "  g - Grip\n";
            std::cout << "  p - Print_state\n";
            std::cout << "  o - Open\n";
            std::cout << "  c - Close\n";
            std::cout << "  q - Quit\n";
            std::cout << "  s - Stop\n";
            lastState = state; 
            stiffness_changed = false;
            std::cout << "kp: " << kp << " kd: " << kd << std::endl;
        }


        // print the current state
        switch (state) {
            case INIT:
                std::cout << "Initializing..." << std::endl;
                currentState = ROTATE;
                break;
            case ROTATE:
                rotateMotors(input == "L");
                break;
            case GRIP:
                gripHand(input == "L");
                break;
            case STOP:
                stopMotors();
                break;
            case PRINT:
                // print the current state
                printState(input == "L", state);
                break;
            case OPEN:
                moveHand(input == "L", true);
                break;
            case CLOSE:
                moveHand(input == "L", false);
                break;
            default:
                std::cout << "Invalid state!" << std::endl;
                inputThread.join();  
                break;
        }
    }

    return 0;
}