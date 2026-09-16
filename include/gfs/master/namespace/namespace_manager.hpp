#pragma once

#include <cstddef>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

namespace gfs::master::namespace_management {

class NamespaceManager {
public:
    enum class NodeType {
        Directory,
        File
    };

    struct NodeInfo {
        std::string path;
        NodeType type = NodeType::Directory;
    };

    NamespaceManager();
    ~NamespaceManager();

    NamespaceManager(const NamespaceManager&) = delete;
    NamespaceManager& operator=(const NamespaceManager&) = delete;
    NamespaceManager(NamespaceManager&&) = delete;
    NamespaceManager& operator=(NamespaceManager&&) = delete;

    bool Exists(const std::string& path) const;
    bool IsDirectory(const std::string& path) const;
    bool IsFile(const std::string& path) const;

    bool CreateDirectory(const std::string& path);
    bool CreateFile(const std::string& path);

    bool DeleteDirectory(const std::string& path);
    bool DeleteFile(const std::string& path);

    bool Rename(
        const std::string& source_path,
        const std::string& destination_path);

    std::vector<NodeInfo> ListDirectory(
        const std::string& path) const;

    std::string ParentPath(const std::string& path) const;
    std::string BaseName(const std::string& path) const;

    static bool IsValidPath(const std::string& path);
    static bool IsRootPath(const std::string& path);

    std::size_t NodeCount() const;

private:
    struct Node {
        std::string name;
        NodeType type = NodeType::Directory;
        Node* parent = nullptr;

        std::vector<std::unique_ptr<Node>> children;

        mutable std::shared_mutex mutex;
    };

    Node* FindNode(const std::string& path);
    const Node* FindNode(const std::string& path) const;

    Node* FindChild(Node* parent, const std::string& name);
    const Node* FindChild(
        const Node* parent,
        const std::string& name) const;

    static std::vector<std::string> SplitPath(
        const std::string& path);

    static bool IsDirectChild(
        const Node* parent,
        const Node* child);

    std::unique_ptr<Node> root_;
};

}  // namespace gfs::master::namespace_management