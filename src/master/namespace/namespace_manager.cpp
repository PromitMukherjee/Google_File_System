// src/master/namespace/namespace_manager.cpp

#include "gfs/master/namespace/namespace_manager.hpp"

#include <algorithm>
#include <cctype>
#include <mutex>
#include <utility>

namespace gfs::master::namespace_management {

NamespaceManager::NamespaceManager()
    : root_(std::make_unique<Node>()) {
    root_->name = "/";
    root_->type = NodeType::Directory;
}

NamespaceManager::~NamespaceManager() = default;

bool NamespaceManager::Exists(const std::string& path) const {
    if (!IsValidPath(path)) {
        return false;
    }

    return FindNode(path) != nullptr;
}

bool NamespaceManager::IsDirectory(const std::string& path) const {
    const Node* node = FindNode(path);
    return node != nullptr && node->type == NodeType::Directory;
}

bool NamespaceManager::IsFile(const std::string& path) const {
    const Node* node = FindNode(path);
    return node != nullptr && node->type == NodeType::File;
}

bool NamespaceManager::CreateDirectory(const std::string& path) {
    if (!IsValidPath(path) || IsRootPath(path) || Exists(path)) {
        return false;
    }

    const std::string parent_path = ParentPath(path);
    const std::string name = BaseName(path);

    Node* parent = FindNode(parent_path);
    if (parent == nullptr || parent->type != NodeType::Directory) {
        return false;
    }

    std::unique_lock parent_lock(parent->mutex);

    if (FindChild(parent, name) != nullptr) {
        return false;
    }

    auto child = std::make_unique<Node>();
    child->name = name;
    child->type = NodeType::Directory;
    child->parent = parent;

    parent->children.push_back(std::move(child));
    return true;
}

bool NamespaceManager::CreateFile(const std::string& path) {
    if (!IsValidPath(path) || IsRootPath(path) || Exists(path)) {
        return false;
    }

    const std::string parent_path = ParentPath(path);
    const std::string name = BaseName(path);

    Node* parent = FindNode(parent_path);
    if (parent == nullptr || parent->type != NodeType::Directory) {
        return false;
    }

    std::unique_lock parent_lock(parent->mutex);

    if (FindChild(parent, name) != nullptr) {
        return false;
    }

    auto child = std::make_unique<Node>();
    child->name = name;
    child->type = NodeType::File;
    child->parent = parent;

    parent->children.push_back(std::move(child));
    return true;
}

bool NamespaceManager::DeleteDirectory(const std::string& path) {
    if (!IsValidPath(path) || IsRootPath(path)) {
        return false;
    }

    Node* node = FindNode(path);
    if (node == nullptr ||
        node->type != NodeType::Directory ||
        !node->children.empty()) {
        return false;
    }

    Node* parent = node->parent;
    if (parent == nullptr) {
        return false;
    }

    std::unique_lock parent_lock(parent->mutex);

    const auto it = std::find_if(
        parent->children.begin(),
        parent->children.end(),
        [node](const std::unique_ptr<Node>& child) {
            return child.get() == node;
        });

    if (it == parent->children.end()) {
        return false;
    }

    parent->children.erase(it);
    return true;
}

bool NamespaceManager::DeleteFile(const std::string& path) {
    if (!IsValidPath(path) || IsRootPath(path)) {
        return false;
    }

    Node* node = FindNode(path);
    if (node == nullptr || node->type != NodeType::File) {
        return false;
    }

    Node* parent = node->parent;
    if (parent == nullptr) {
        return false;
    }

    std::unique_lock parent_lock(parent->mutex);

    const auto it = std::find_if(
        parent->children.begin(),
        parent->children.end(),
        [node](const std::unique_ptr<Node>& child) {
            return child.get() == node;
        });

    if (it == parent->children.end()) {
        return false;
    }

    parent->children.erase(it);
    return true;
}

bool NamespaceManager::Rename(
    const std::string& source_path,
    const std::string& destination_path) {

    if (!IsValidPath(source_path) ||
        !IsValidPath(destination_path) ||
        IsRootPath(source_path) ||
        IsRootPath(destination_path) ||
        source_path == destination_path) {
        return false;
    }

    Node* source = FindNode(source_path);
    if (source == nullptr) {
        return false;
    }

    if (FindNode(destination_path) != nullptr) {
        return false;
    }

    Node* destination_parent =
        FindNode(ParentPath(destination_path));

    if (destination_parent == nullptr ||
        destination_parent->type != NodeType::Directory) {
        return false;
    }

    if (source->type == NodeType::Directory) {
        Node* ancestor = destination_parent;

        while (ancestor != nullptr) {
            if (ancestor == source) {
                return false;
            }

            ancestor = ancestor->parent;
        }
    }

    Node* source_parent = source->parent;
    if (source_parent == nullptr) {
        return false;
    }

    const std::string destination_name =
        BaseName(destination_path);

    if (source_parent == destination_parent) {
        std::unique_lock parent_lock(source_parent->mutex);

        if (FindChild(destination_parent, destination_name) != nullptr) {
            return false;
        }

        const auto it = std::find_if(
            source_parent->children.begin(),
            source_parent->children.end(),
            [source](const std::unique_ptr<Node>& child) {
                return child.get() == source;
            });

        if (it == source_parent->children.end()) {
            return false;
        }

        (*it)->name = destination_name;
        return true;
    }

    std::scoped_lock lock(
        source_parent->mutex,
        destination_parent->mutex);

    if (FindChild(destination_parent, destination_name) != nullptr) {
        return false;
    }

    auto it = std::find_if(
        source_parent->children.begin(),
        source_parent->children.end(),
        [source](const std::unique_ptr<Node>& child) {
            return child.get() == source;
        });

    if (it == source_parent->children.end()) {
        return false;
    }

    std::unique_ptr<Node> moved = std::move(*it);
    source_parent->children.erase(it);

    moved->name = destination_name;
    moved->parent = destination_parent;

    destination_parent->children.push_back(std::move(moved));
    return true;
}

std::vector<NamespaceManager::NodeInfo>
NamespaceManager::ListDirectory(const std::string& path) const {

    const Node* node = FindNode(path);
    if (node == nullptr || node->type != NodeType::Directory) {
        return {};
    }

    std::shared_lock lock(node->mutex);

    std::vector<NodeInfo> result;
    result.reserve(node->children.size());

    for (const auto& child : node->children) {
        NodeInfo info;
        info.path = path == "/"
            ? "/" + child->name
            : path + "/" + child->name;
        info.type = child->type;

        result.push_back(std::move(info));
    }

    return result;
}

std::string NamespaceManager::ParentPath(
    const std::string& path) const {

    if (!IsValidPath(path) || IsRootPath(path)) {
        return {};
    }

    const std::size_t separator = path.find_last_of('/');
    if (separator == 0) {
        return "/";
    }

    return path.substr(0, separator);
}

std::string NamespaceManager::BaseName(
    const std::string& path) const {

    if (!IsValidPath(path) || IsRootPath(path)) {
        return {};
    }

    const std::size_t separator = path.find_last_of('/');
    return path.substr(separator + 1);
}

bool NamespaceManager::IsValidPath(const std::string& path) {
    if (path.empty() ||
        path.front() != '/' ||
        path.find('\0') != std::string::npos) {
        return false;
    }

    if (path.size() > 1 && path.back() == '/') {
        return false;
    }

    std::size_t start = 1;

    while (start < path.size()) {
        const std::size_t end = path.find('/', start);
        const std::size_t length =
            end == std::string::npos
                ? path.size() - start
                : end - start;

        if (length == 0) {
            return false;
        }

        const std::string component =
            path.substr(start, length);

        if (component == "." || component == "..") {
            return false;
        }

        start = end == std::string::npos
            ? path.size()
            : end + 1;
    }

    return true;
}

bool NamespaceManager::IsRootPath(const std::string& path) {
    return path == "/";
}

std::size_t NamespaceManager::NodeCount() const {
    std::size_t count = 0;

    std::vector<const Node*> stack;
    stack.push_back(root_.get());

    while (!stack.empty()) {
        const Node* node = stack.back();
        stack.pop_back();

        ++count;

        std::shared_lock lock(node->mutex);

        for (const auto& child : node->children) {
            stack.push_back(child.get());
        }
    }

    return count;
}

NamespaceManager::Node*
NamespaceManager::FindNode(const std::string& path) {
    if (!IsValidPath(path)) {
        return nullptr;
    }

    if (IsRootPath(path)) {
        return root_.get();
    }

    Node* current = root_.get();

    for (const std::string& component : SplitPath(path)) {
        current = FindChild(current, component);

        if (current == nullptr) {
            return nullptr;
        }
    }

    return current;
}

const NamespaceManager::Node*
NamespaceManager::FindNode(const std::string& path) const {
    if (!IsValidPath(path)) {
        return nullptr;
    }

    if (IsRootPath(path)) {
        return root_.get();
    }

    const Node* current = root_.get();

    for (const std::string& component : SplitPath(path)) {
        current = FindChild(current, component);

        if (current == nullptr) {
            return nullptr;
        }
    }

    return current;
}

NamespaceManager::Node*
NamespaceManager::FindChild(
    Node* parent,
    const std::string& name) {

    if (parent == nullptr) {
        return nullptr;
    }

    for (const auto& child : parent->children) {
        if (child->name == name) {
            return child.get();
        }
    }

    return nullptr;
}

const NamespaceManager::Node*
NamespaceManager::FindChild(
    const Node* parent,
    const std::string& name) const {

    if (parent == nullptr) {
        return nullptr;
    }

    for (const auto& child : parent->children) {
        if (child->name == name) {
            return child.get();
        }
    }

    return nullptr;
}

std::vector<std::string>
NamespaceManager::SplitPath(const std::string& path) {

    std::vector<std::string> components;

    if (!IsValidPath(path) || IsRootPath(path)) {
        return components;
    }

    std::size_t start = 1;

    while (start < path.size()) {
        const std::size_t end = path.find('/', start);

        if (end == std::string::npos) {
            components.push_back(path.substr(start));
            break;
        }

        components.push_back(
            path.substr(start, end - start));

        start = end + 1;
    }

    return components;
}

bool NamespaceManager::IsDirectChild(
    const Node* parent,
    const Node* child) {

    return child != nullptr && child->parent == parent;
}

}  // namespace gfs::master::namespace_management