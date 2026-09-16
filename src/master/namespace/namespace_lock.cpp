#include "gfs/master/namespace/namespace_lock.hpp"

#include <algorithm>
#include <mutex>
#include <utility>

namespace gfs::master::namespace_management {

NamespaceLock::NamespaceLock()
    : root_(std::make_shared<LockNode>()) {
    root_->name = "/";
}

NamespaceLock::~NamespaceLock() = default;

NamespaceLock::ReadGuard
NamespaceLock::AcquireRead(const std::string& path) {
    return AcquireReadPath({path});
}

NamespaceLock::WriteGuard
NamespaceLock::AcquireWrite(const std::string& path) {
    return AcquireWritePath({path});
}

NamespaceLock::ReadGuard
NamespaceLock::AcquireReadPath(
    const std::vector<std::string>& paths) {

    std::vector<std::shared_ptr<LockNode>> nodes;

    for (const std::string& path : paths) {
        const auto node = GetOrCreateNode(path);

        if (node != nullptr) {
            nodes.push_back(node);
        }
    }

    std::sort(
        nodes.begin(),
        nodes.end(),
        [](const auto& lhs, const auto& rhs) {
            return lhs.get() < rhs.get();
        });

    nodes.erase(
        std::unique(
            nodes.begin(),
            nodes.end()),
        nodes.end());

    return ReadGuard(std::move(nodes));
}

NamespaceLock::WriteGuard
NamespaceLock::AcquireWritePath(
    const std::vector<std::string>& paths) {

    std::vector<std::shared_ptr<LockNode>> nodes;

    for (const std::string& path : paths) {
        const auto node = GetOrCreateNode(path);

        if (node != nullptr) {
            nodes.push_back(node);
        }
    }

    std::sort(
        nodes.begin(),
        nodes.end(),
        [](const auto& lhs, const auto& rhs) {
            return lhs.get() < rhs.get();
        });

    nodes.erase(
        std::unique(
            nodes.begin(),
            nodes.end()),
        nodes.end());

    return WriteGuard(std::move(nodes));
}

std::shared_ptr<NamespaceLock::LockNode>
NamespaceLock::GetOrCreateNode(const std::string& path) {

    if (path.empty() || path.front() != '/') {
        return nullptr;
    }

    std::shared_ptr<LockNode> current = root_;

    std::size_t start = 1;

    std::unique_lock tree_lock(tree_mutex_);

    while (start < path.size()) {
        const std::size_t end = path.find('/', start);
        const std::size_t length =
            end == std::string::npos
                ? path.size() - start
                : end - start;

        if (length == 0) {
            return nullptr;
        }

        const std::string component =
            path.substr(start, length);

        auto child_it = std::find_if(
            current->children.begin(),
            current->children.end(),
            [&component](const auto& child) {
                return child->name == component;
            });

        if (child_it == current->children.end()) {
            auto child = std::make_shared<LockNode>();
            child->name = component;
            child->parent = current;

            current->children.push_back(child);
            current = std::move(child);
        } else {
            current = *child_it;
        }

        if (end == std::string::npos) {
            break;
        }

        start = end + 1;
    }

    return current;
}

std::shared_ptr<const NamespaceLock::LockNode>
NamespaceLock::FindNode(const std::string& path) const {

    if (path.empty() || path.front() != '/') {
        return nullptr;
    }

    std::shared_ptr<const LockNode> current = root_;

    if (path == "/") {
        return current;
    }

    std::size_t start = 1;

    std::shared_lock tree_lock(tree_mutex_);

    while (start < path.size()) {
        const std::size_t end = path.find('/', start);
        const std::size_t length =
            end == std::string::npos
                ? path.size() - start
                : end - start;

        if (length == 0) {
            return nullptr;
        }

        const std::string component =
            path.substr(start, length);

        const auto child_it = std::find_if(
            current->children.begin(),
            current->children.end(),
            [&component](const auto& child) {
                return child->name == component;
            });

        if (child_it == current->children.end()) {
            return nullptr;
        }

        current = *child_it;

        if (end == std::string::npos) {
            break;
        }

        start = end + 1;
    }

    return current;
}

std::vector<std::string>
NamespaceLock::SplitPath(const std::string& path) {

    std::vector<std::string> components;

    if (path.empty() || path == "/" || path.front() != '/') {
        return components;
    }

    std::size_t start = 1;

    while (start < path.size()) {
        const std::size_t end = path.find('/', start);

        if (end == std::string::npos) {
            components.push_back(path.substr(start));
            break;
        }

        if (end > start) {
            components.push_back(
                path.substr(start, end - start));
        }

        start = end + 1;
    }

    return components;
}

NamespaceLock::ReadGuard::ReadGuard(
    std::vector<std::shared_ptr<LockNode>> nodes)
    : nodes_(std::move(nodes)) {

    locks_.reserve(nodes_.size());

    for (const auto& node : nodes_) {
        locks_.push_back(
            std::make_unique<
                std::shared_lock<std::shared_mutex>>(node->mutex));
    }
}

NamespaceLock::ReadGuard::~ReadGuard() = default;

NamespaceLock::ReadGuard::ReadGuard(
    ReadGuard&& other) noexcept
    : nodes_(std::move(other.nodes_)),
      locks_(std::move(other.locks_)) {
}

NamespaceLock::ReadGuard&
NamespaceLock::ReadGuard::operator=(
    ReadGuard&& other) noexcept {

    if (this != &other) {
        locks_.clear();
        nodes_.clear();

        nodes_ = std::move(other.nodes_);
        locks_ = std::move(other.locks_);
    }

    return *this;
}

bool NamespaceLock::ReadGuard::OwnsLock() const noexcept {
    return !locks_.empty();
}

NamespaceLock::WriteGuard::WriteGuard(
    std::vector<std::shared_ptr<LockNode>> nodes)
    : nodes_(std::move(nodes)) {

    locks_.reserve(nodes_.size());

    for (const auto& node : nodes_) {
        locks_.push_back(
            std::make_unique<
                std::unique_lock<std::shared_mutex>>(node->mutex));
    }
}

NamespaceLock::WriteGuard::~WriteGuard() = default;

NamespaceLock::WriteGuard::WriteGuard(
    WriteGuard&& other) noexcept
    : nodes_(std::move(other.nodes_)),
      locks_(std::move(other.locks_)) {
}

NamespaceLock::WriteGuard&
NamespaceLock::WriteGuard::operator=(
    WriteGuard&& other) noexcept {

    if (this != &other) {
        locks_.clear();
        nodes_.clear();

        nodes_ = std::move(other.nodes_);
        locks_ = std::move(other.locks_);
    }

    return *this;
}

bool NamespaceLock::WriteGuard::OwnsLock() const noexcept {
    return !locks_.empty();
}

}  // namespace gfs::master::namespace_management