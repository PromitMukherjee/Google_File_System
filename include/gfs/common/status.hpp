#pragma once

#include <string>

namespace gfs {

enum class StatusCode {
    kOk = 0,
    kInvalidArgument,
    kNotFound,
    kAlreadyExists,
    kPermissionDenied,
    kUnavailable,
    kIOError,
    kInternalError,
    kUnknown
};

class Status {
public:
    Status() noexcept;

    Status(StatusCode code, std::string message);

    static Status OK();
    static Status InvalidArgument(std::string message);
    static Status NotFound(std::string message);
    static Status AlreadyExists(std::string message);
    static Status PermissionDenied(std::string message);
    static Status Unavailable(std::string message);
    static Status IOError(std::string message);
    static Status InternalError(std::string message);

    bool ok() const noexcept;

    StatusCode code() const noexcept;

    const std::string& message() const noexcept;

    explicit operator bool() const noexcept;

private:
    StatusCode code_;
    std::string message_;
};

}  // namespace gfs