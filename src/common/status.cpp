#include "gfs/common/status.hpp"

#include <utility>

namespace gfs {

Status::Status() noexcept
    : code_(StatusCode::kOk) {}

Status::Status(StatusCode code, std::string message)
    : code_(code),
      message_(std::move(message)) {}

Status Status::OK() {
    return Status{};
}

Status Status::InvalidArgument(std::string message) {
    return Status(StatusCode::kInvalidArgument, std::move(message));
}

Status Status::NotFound(std::string message) {
    return Status(StatusCode::kNotFound, std::move(message));
}

Status Status::AlreadyExists(std::string message) {
    return Status(StatusCode::kAlreadyExists, std::move(message));
}

Status Status::PermissionDenied(std::string message) {
    return Status(StatusCode::kPermissionDenied, std::move(message));
}

Status Status::Unavailable(std::string message) {
    return Status(StatusCode::kUnavailable, std::move(message));
}

Status Status::IOError(std::string message) {
    return Status(StatusCode::kIOError, std::move(message));
}

Status Status::InternalError(std::string message) {
    return Status(StatusCode::kInternalError, std::move(message));
}

bool Status::ok() const noexcept {
    return code_ == StatusCode::kOk;
}

StatusCode Status::code() const noexcept {
    return code_;
}

const std::string& Status::message() const noexcept {
    return message_;
}

Status::operator bool() const noexcept {
    return ok();
}

}  // namespace gfs