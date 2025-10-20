#ifndef SDR_KEYBOARD_COMMANDER_HPP_
#define SDR_KEYBOARD_COMMANDER_HPP_

#include "rclcpp/rclcpp.hpp"
#include "lifecycle_msgs/srv/change_state.hpp"
#include "lifecycle_msgs/srv/get_state.hpp"
#include "lifecycle_msgs/msg/transition.hpp"
#include "lifecycle_msgs/msg/state.hpp"

#include <termios.h> // For non-blocking keyboard input
#include <unistd.h>  // For STDIN_FILENO
#include <string>
#include <map>

// Define aliases for convenience
using ChangeState = lifecycle_msgs::srv::ChangeState;
using GetState = lifecycle_msgs::srv::GetState;
using Transition = lifecycle_msgs::msg::Transition;
using State = lifecycle_msgs::msg::State;

namespace sdr
{

    class SDRKeyboardCommander : public rclcpp::Node
    {
    public:
        explicit SDRKeyboardCommander(const rclcpp::NodeOptions &options);
        virtual ~SDRKeyboardCommander();

    private:
        // --- Keyboard Handling ---
        /**
         * @brief Configures the terminal for non-blocking, non-echoing input.
         */
        void init_keyboard();

        /**
         * @brief Restores the terminal to its original settings.
         */
        void restore_keyboard();

        /**
         * @brief Polls the keyboard for a single character.
         * @return The character pressed, or -1 if no key was pressed.
         */
        int getch();

        /**
         * @brief Timer callback to check for keyboard input and process it.
         */
        void check_keyboard();

        // --- ROS 2 Lifecycle Client Functions ---
        /**
         * @brief Sends a request to the /sdr/change_state service.
         * @param transition_id The ID of the transition to request (e.g., Transition::TRANSITION_CONFIGURE).
         */
        void request_transition(uint8_t transition_id);

        /**
         * @brief Sends a request to the /sdr/get_state service.
         */
        void get_current_state();

        /**
         * @brief Prints the help menu to the console.
         */
        void print_help();

        // --- Member Variables ---
        rclcpp::Client<ChangeState>::SharedPtr change_state_client_;
        rclcpp::Client<GetState>::SharedPtr get_state_client_;
        rclcpp::TimerBase::SharedPtr timer_;

        // For keyboard input
        struct termios old_tio_, new_tio_;

        // For logging
        std::map<uint8_t, std::string> transition_map_;
        std::string target_node_name_ = "sdr";
    };

} // namespace sdr

#endif // SDR_KEYBOARD_COMMANDER_HPP_