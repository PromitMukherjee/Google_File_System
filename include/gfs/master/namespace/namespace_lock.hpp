#pragma once

#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>
#include <mutex>

namespace gfs::master::namespace_management {

class NamespaceLock {
public:
    class ReadGuard;
    class WriteGuard;

    NamespaceLock();
    ~NamespaceLock();

    NamespaceLock(const NamespaceLock&) = delete;
    NamespaceLock& operator=(const NamespaceLock&) = delete;
    NamespaceLock(NamespaceLock&&) = delete;
    NamespaceLock& operator=(NamespaceLock&&) = delete;

    ReadGuard AcquireRead(const std::string& path);
    WriteGuard AcquireWrite(const std::string& path);

    ReadGuard AcquireReadPath(
        const std::vector<std::string>& paths);

    WriteGuard AcquireWritePath(
        const std::vector<std::string>& paths);

private:
    struct LockNode {
        std::shared_ptr<LockNode> parent;
        std::string name;

        std::vector<std::shared_ptr<LockNode>> children;

        mutable std::shared_mutex mutex;
    };

    std::shared_ptr<LockNode> GetOrCreateNode(
        const std::string& path);

    std::shared_ptr<const LockNode> FindNode(
        const std::string& path) const;

    static std::vector<std::string> SplitPath(
        const std::string& path);

    std::shared_ptr<LockNode> root_;

    mutable std::shared_mutex tree_mutex_;

public:
    class ReadGuard {
    public:
        ReadGuard() = default;

        explicit ReadGuard(
            std::vector<std::shared_ptr<LockNode>> nodes);

        ~ReadGuard();

        ReadGuard(const ReadGuard&) = delete;
        ReadGuard& operator=(const ReadGuard&) = delete;

        ReadGuard(ReadGuard&& other) noexcept;
        ReadGuard& operator=(ReadGuard&& other) noexcept;

        bool OwnsLock() const noexcept;

    private:
        std::vector<std::shared_ptr<LockNode>> nodes_;
        std::vector<std::unique_ptr<
            std::shared_lock<std::shared_mutex>>> locks_;
    };

    class WriteGuard {
    public:
        WriteGuard() = default;

        explicit WriteGuard(
            std::vector<std::shared_ptr<LockNode>> nodes);

        ~WriteGuard();

        WriteGuard(const WriteGuard&) = delete;
        WriteGuard& operator=(const WriteGuard&) = delete;

        WriteGuard(WriteGuard&& other) noexcept;
        WriteGuard& operator=(WriteGuard&& other) noexcept;

        bool OwnsLock() const noexcept;

    private:
        std::vector<std::shared_ptr<LockNode>> nodes_;
        std::vector<std::unique_ptr<
            std::unique_lock<std::shared_mutex>>> locks_;
    };
};

}  // namespace gfs::master::namespace_management