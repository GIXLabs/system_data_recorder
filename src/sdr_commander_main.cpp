#include "sdr/sdr_keyboard_commander.hpp" // Adjust this path
#include "rclcpp/rclcpp.hpp"

int main(int argc, char *argv[])
{
    rclcpp::init(argc, argv);

    rclcpp::NodeOptions options;
    auto commander_node = std::make_shared<sdr::SDRKeyboardCommander>(options);

    rclcpp::spin(commander_node);

    rclcpp::shutdown();
    return 0;
}