// SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
// SPDX-FileCopyrightText: All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <deque>
#include <limits>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace py = pybind11;

namespace {

// -----------------------------------------------------------------------------
// Constants and Small Geometry Helpers
// -----------------------------------------------------------------------------
constexpr int8_t kVertexOrphan = 0;
constexpr int8_t kVertexFront = 1;
constexpr int8_t kVertexInner = 2;

constexpr int8_t kEdgeBorder = 0;
constexpr int8_t kEdgeFront = 1;
constexpr int8_t kEdgeInner = 2;

constexpr double kEps = 1.0e-12;
constexpr double kPi = 3.1415926535897932384626433832795;

struct Vec3 {
    double x;
    double y;
    double z;

    Vec3() : x(0.0), y(0.0), z(0.0) {}
    Vec3(double x_, double y_, double z_) : x(x_), y(y_), z(z_) {}

    Vec3 operator+(const Vec3 &rhs) const { return Vec3{x + rhs.x, y + rhs.y, z + rhs.z}; }
    Vec3 operator-(const Vec3 &rhs) const { return Vec3{x - rhs.x, y - rhs.y, z - rhs.z}; }
    Vec3 operator*(double s) const { return Vec3{x * s, y * s, z * s}; }
    Vec3 operator/(double s) const { return Vec3{x / s, y / s, z / s}; }

    Vec3 &operator+=(const Vec3 &rhs) {
        x += rhs.x;
        y += rhs.y;
        z += rhs.z;
        return *this;
    }
};

inline double Dot(const Vec3 &a, const Vec3 &b) { return a.x * b.x + a.y * b.y + a.z * b.z; }

inline Vec3 Cross(const Vec3 &a, const Vec3 &b) {
    return Vec3{
        a.y * b.z - a.z * b.y,
        a.z * b.x - a.x * b.z,
        a.x * b.y - a.y * b.x,
    };
}

inline double Norm(const Vec3 &v) { return std::sqrt(std::max(0.0, Dot(v, v))); }

inline Vec3 NormalizeOrZero(const Vec3 &v) {
    const double n = Norm(v);
    if (n <= kEps) {
        return Vec3{};
    }
    return v / n;
}

// -----------------------------------------------------------------------------
// Topology Records
// -----------------------------------------------------------------------------
struct EdgeRecord {
    int source;
    int target;
    int triangle0;
    int triangle1;
    int8_t edge_type;
};

struct TriangleRecord {
    int v0;
    int v1;
    int v2;
    Vec3 ball_center;
};

struct CandidateResult {
    int candidate;
    Vec3 center;
    double angle;
};

// -----------------------------------------------------------------------------
// Ball Pivoting State
// -----------------------------------------------------------------------------
class BallPivotState {
public:
    BallPivotState(
        std::vector<Vec3> points,
        std::vector<Vec3> normals,
        std::vector<int32_t> row_ptr,
        std::vector<int32_t> col_idx,
        int max_triangles)
        : points_(std::move(points)),
          normals_(std::move(normals)),
          row_ptr_(std::move(row_ptr)),
          col_idx_(std::move(col_idx)),
          n_points_(static_cast<int>(points_.size())),
          max_triangles_(max_triangles),
          vertex_types_(n_points_, kVertexOrphan),
          vertex_edges_(n_points_),
          candidate_mark_(n_points_, 0),
          candidate_tag_(1),
          batch_vertex_mark_(n_points_, 0),
          batch_vertex_tag_(1) {}

    std::vector<int32_t> Run(const std::vector<double> &radii, const std::string &front_mode, int front_batch_size) {
        faces_.clear();

        for (double radius : radii) {
            RefreshBorderEdgesForRadius(radius);

            if (edge_front_.empty()) {
                FindSeedTriangle(radius, front_mode, front_batch_size);
            } else {
                ExpandTriangulationWithMode(radius, front_mode, front_batch_size);
            }

            if (max_triangles_ > 0 && static_cast<int>(faces_.size()) >= max_triangles_) {
                break;
            }
        }

        std::vector<int32_t> out;
        out.reserve(faces_.size() * 3);
        for (const auto &f : faces_) {
            out.push_back(static_cast<int32_t>(f[0]));
            out.push_back(static_cast<int32_t>(f[1]));
            out.push_back(static_cast<int32_t>(f[2]));
        }
        return out;
    }

private:
    // -------------------------------------------------------------------------
    // Geometry Helpers
    // -------------------------------------------------------------------------
    Vec3 FaceNormal(int v0, int v1, int v2) const {
        const Vec3 n = Cross(points_[v1] - points_[v0], points_[v2] - points_[v0]);
        const double nn = Norm(n);
        if (nn <= kEps) {
            return Vec3{};
        }
        return n / nn;
    }

    bool IsCompatible(int v0, int v1, int v2) const {
        Vec3 normal = FaceNormal(v0, v1, v2);
        if (Dot(normal, normals_[v0]) < -1.0e-16) {
            normal = normal * -1.0;
        }
        return Dot(normal, normals_[v0]) > -1.0e-16 && Dot(normal, normals_[v1]) > -1.0e-16 &&
               Dot(normal, normals_[v2]) > -1.0e-16;
    }

    std::pair<bool, Vec3> ComputeBallCenter(int v0, int v1, int v2, double radius) const {
        const Vec3 p0 = points_[v0];
        const Vec3 p1 = points_[v1];
        const Vec3 p2 = points_[v2];

        double c = Dot(p1 - p0, p1 - p0);
        double b = Dot(p0 - p2, p0 - p2);
        double a = Dot(p2 - p1, p2 - p1);

        double alpha = a * (b + c - a);
        double beta = b * (a + c - b);
        double gamma = c * (a + b - c);
        const double abg = alpha + beta + gamma;
        if (abg < 1.0e-16) {
            return {false, Vec3{}};
        }

        alpha /= abg;
        beta /= abg;
        gamma /= abg;

        const Vec3 circ_center = p0 * alpha + p1 * beta + p2 * gamma;

        const double aa = std::sqrt(std::max(a, 0.0));
        const double bb = std::sqrt(std::max(b, 0.0));
        const double cc = std::sqrt(std::max(c, 0.0));
        const double denom = (aa + bb + cc) * (bb + cc - aa) * (cc + aa - bb) * (aa + bb - cc);
        if (std::abs(denom) <= kEps) {
            return {false, Vec3{}};
        }

        double circ_radius_sq = a * b * c;
        circ_radius_sq /= denom;

        const double height_sq = radius * radius - circ_radius_sq;
        if (height_sq < 0.0) {
            return {false, Vec3{}};
        }

        Vec3 tri_norm = Cross(p1 - p0, p2 - p0);
        const double tri_norm_norm = Norm(tri_norm);
        if (tri_norm_norm <= kEps) {
            return {false, Vec3{}};
        }
        tri_norm = tri_norm / tri_norm_norm;

        Vec3 point_norm = normals_[v0] + normals_[v1] + normals_[v2];
        const double point_norm_norm = Norm(point_norm);
        if (point_norm_norm <= kEps) {
            return {false, Vec3{}};
        }
        point_norm = point_norm / point_norm_norm;

        if (Dot(tri_norm, point_norm) < 0.0) {
            tri_norm = tri_norm * -1.0;
        }

        const Vec3 center = circ_center + tri_norm * std::sqrt(std::max(height_sq, 0.0));
        return {true, center};
    }

    // -------------------------------------------------------------------------
    // Edge / Vertex Topology Helpers
    // -------------------------------------------------------------------------
    static uint64_t EdgeKey(int v0, int v1) {
        const uint32_t a = static_cast<uint32_t>(std::min(v0, v1));
        const uint32_t b = static_cast<uint32_t>(std::max(v0, v1));
        return (static_cast<uint64_t>(a) << 32U) | static_cast<uint64_t>(b);
    }

    int GetLinkingEdge(int v0, int v1) const {
        const auto it = edge_lookup_.find(EdgeKey(v0, v1));
        if (it == edge_lookup_.end()) {
            return -1;
        }
        return it->second;
    }

    int GetOrCreateEdge(int v0, int v1) {
        const uint64_t key = EdgeKey(v0, v1);
        const auto it = edge_lookup_.find(key);
        if (it != edge_lookup_.end()) {
            return it->second;
        }

        const int edge_idx = static_cast<int>(edges_.size());
        edges_.push_back(EdgeRecord{
            .source = v0,
            .target = v1,
            .triangle0 = -1,
            .triangle1 = -1,
            .edge_type = kEdgeFront,
        });
        edge_lookup_[key] = edge_idx;
        return edge_idx;
    }

    int EdgeOppositeVertex(int edge_idx) const {
        const EdgeRecord &edge = edges_[edge_idx];
        if (edge.triangle0 < 0) {
            return -1;
        }

        const TriangleRecord &tri = triangles_[edge.triangle0];
        if (tri.v0 != edge.source && tri.v0 != edge.target) {
            return tri.v0;
        }
        if (tri.v1 != edge.source && tri.v1 != edge.target) {
            return tri.v1;
        }
        return tri.v2;
    }

    void AddAdjacentTriangle(int edge_idx, int tri_idx) {
        EdgeRecord &edge = edges_[edge_idx];
        if (edge.triangle0 == tri_idx || edge.triangle1 == tri_idx) {
            return;
        }

        if (edge.triangle0 < 0) {
            edge.triangle0 = tri_idx;
            edge.edge_type = kEdgeFront;

            const int opp = EdgeOppositeVertex(edge_idx);
            if (opp >= 0) {
                Vec3 tri_norm = Cross(points_[edge.target] - points_[edge.source], points_[opp] - points_[edge.source]);
                const double tri_norm_norm = Norm(tri_norm);
                if (tri_norm_norm > kEps) {
                    tri_norm = tri_norm / tri_norm_norm;
                    Vec3 pt_norm = normals_[edge.source] + normals_[edge.target] + normals_[opp];
                    const double pt_norm_norm = Norm(pt_norm);
                    if (pt_norm_norm > kEps) {
                        pt_norm = pt_norm / pt_norm_norm;
                        if (Dot(pt_norm, tri_norm) < 0.0) {
                            std::swap(edge.source, edge.target);
                        }
                    }
                }
            }
            return;
        }

        if (edge.triangle1 < 0) {
            edge.triangle1 = tri_idx;
            edge.edge_type = kEdgeInner;
        }
    }

    void UpdateVertexType(int vertex_idx) {
        const auto &edges = vertex_edges_[vertex_idx];
        if (edges.empty()) {
            vertex_types_[vertex_idx] = kVertexOrphan;
            return;
        }

        for (int edge_idx : edges) {
            if (edges_[edge_idx].edge_type != kEdgeInner) {
                vertex_types_[vertex_idx] = kVertexFront;
                return;
            }
        }
        vertex_types_[vertex_idx] = kVertexInner;
    }

    // -------------------------------------------------------------------------
    // Candidate Search
    // -------------------------------------------------------------------------
    bool CreateTriangle(int v0, int v1, int v2, const Vec3 &center) {
        if (max_triangles_ > 0 && static_cast<int>(faces_.size()) >= max_triangles_) {
            return false;
        }

        const int tri_idx = static_cast<int>(triangles_.size());
        triangles_.push_back(TriangleRecord{.v0 = v0, .v1 = v1, .v2 = v2, .ball_center = center});

        const int e0 = GetOrCreateEdge(v0, v1);
        AddAdjacentTriangle(e0, tri_idx);
        vertex_edges_[v0].insert(e0);
        vertex_edges_[v1].insert(e0);

        const int e1 = GetOrCreateEdge(v1, v2);
        AddAdjacentTriangle(e1, tri_idx);
        vertex_edges_[v1].insert(e1);
        vertex_edges_[v2].insert(e1);

        const int e2 = GetOrCreateEdge(v2, v0);
        AddAdjacentTriangle(e2, tri_idx);
        vertex_edges_[v2].insert(e2);
        vertex_edges_[v0].insert(e2);

        UpdateVertexType(v0);
        UpdateVertexType(v1);
        UpdateVertexType(v2);

        const Vec3 face_normal = FaceNormal(v0, v1, v2);
        if (Dot(face_normal, normals_[v0]) > -1.0e-16) {
            faces_.push_back({v0, v1, v2});
        } else {
            faces_.push_back({v0, v2, v1});
        }
        return true;
    }

    std::vector<int> CandidatePoolFromVertices(int v0, int v1, int v2, const Vec3 &midpoint, double radius) {
        candidate_tag_ += 1;
        if (candidate_tag_ >= std::numeric_limits<int32_t>::max()) {
            std::fill(candidate_mark_.begin(), candidate_mark_.end(), 0);
            candidate_tag_ = 1;
        }

        const int32_t tag = candidate_tag_;
        std::vector<int> candidates;
        candidates.reserve(256);

        auto append_neighbors = [&](int vertex_idx) {
            const int32_t start = row_ptr_[vertex_idx];
            const int32_t end = row_ptr_[vertex_idx + 1];
            for (int32_t p = start; p < end; ++p) {
                const int idx = static_cast<int>(col_idx_[p]);
                if (idx < 0 || idx >= n_points_) {
                    continue;
                }
                if (candidate_mark_[idx] == tag) {
                    continue;
                }
                candidate_mark_[idx] = tag;
                candidates.push_back(idx);
            }
        };

        append_neighbors(v0);
        append_neighbors(v1);
        if (v2 >= 0) {
            append_neighbors(v2);
        }

        const double max_dist_sq = (2.0 * radius) * (2.0 * radius);
        std::vector<int> filtered;
        filtered.reserve(candidates.size());
        for (int idx : candidates) {
            const Vec3 d = points_[idx] - midpoint;
            if (Dot(d, d) <= max_dist_sq) {
                filtered.push_back(idx);
            }
        }
        return filtered;
    }

    bool IsEmptyBallExcludingThree(
        const Vec3 &center,
        double radius,
        int e0,
        int e1,
        int e2,
        const std::vector<int> &candidates) const {
        const double threshold_sq = (radius - 1.0e-16) * (radius - 1.0e-16);
        for (int idx : candidates) {
            if (idx == e0 || idx == e1 || idx == e2) {
                continue;
            }
            const Vec3 d = center - points_[idx];
            if (Dot(d, d) < threshold_sq) {
                return false;
            }
        }
        return true;
    }

    bool IsEmptyBall(
        const Vec3 &center,
        double radius,
        int e0,
        int e1,
        int e2,
        const std::vector<int> &candidates) const {
        const double threshold_sq = (radius - 1.0e-16) * (radius - 1.0e-16);
        for (int idx : candidates) {
            if (idx == e0 || idx == e1 || idx == e2) {
                continue;
            }
            const Vec3 d = center - points_[idx];
            if (Dot(d, d) < threshold_sq) {
                return false;
            }
        }
        return true;
    }

    std::optional<CandidateResult> FindCandidateVertex(int edge_idx, double radius) {
        const EdgeRecord &edge = edges_[edge_idx];
        if (edge.triangle0 < 0) {
            return std::nullopt;
        }

        const int src = edge.source;
        const int tgt = edge.target;
        const int opp = EdgeOppositeVertex(edge_idx);
        if (opp < 0) {
            return std::nullopt;
        }

        const TriangleRecord &tri = triangles_[edge.triangle0];
        const Vec3 center = tri.ball_center;
        const Vec3 midpoint = (points_[src] + points_[tgt]) * 0.5;

        Vec3 edge_dir = points_[tgt] - points_[src];
        const double edge_norm = Norm(edge_dir);
        if (edge_norm <= kEps) {
            return std::nullopt;
        }
        edge_dir = edge_dir / edge_norm;

        Vec3 a = center - midpoint;
        const double a_norm = Norm(a);
        if (a_norm <= kEps) {
            return std::nullopt;
        }
        a = a / a_norm;

        const std::vector<int> candidates = CandidatePoolFromVertices(src, tgt, opp, midpoint, radius);

        double min_angle = 2.0 * kPi;
        int best_idx = -1;
        Vec3 best_center{};

        for (int candidate : candidates) {
            if (candidate == src || candidate == tgt || candidate == opp) {
                continue;
            }

            auto [valid, new_center] = ComputeBallCenter(src, tgt, candidate, radius);
            if (!valid) {
                continue;
            }

            Vec3 b = new_center - midpoint;
            const double b_norm = Norm(b);
            if (b_norm <= kEps) {
                continue;
            }
            b = b / b_norm;

            const double cos_angle = std::max(-1.0, std::min(1.0, Dot(a, b)));
            double angle = std::acos(cos_angle);

            const Vec3 c = Cross(a, b);
            if (Dot(c, edge_dir) < 0.0) {
                angle = 2.0 * kPi - angle;
            }
            if (angle >= min_angle) {
                continue;
            }

            if (!IsEmptyBallExcludingThree(new_center, radius, src, tgt, candidate, candidates)) {
                continue;
            }

            min_angle = angle;
            best_idx = candidate;
            best_center = new_center;
        }

        if (best_idx < 0) {
            return std::nullopt;
        }

        return CandidateResult{.candidate = best_idx, .center = best_center, .angle = min_angle};
    }

    // -------------------------------------------------------------------------
    // Seed and Front Expansion
    // -------------------------------------------------------------------------
    std::pair<bool, Vec3> TryTriangleSeed(int v0, int v1, int v2, double radius, const std::vector<int> &neighborhood) {
        if (!IsCompatible(v0, v1, v2)) {
            return {false, Vec3{}};
        }

        const int e0 = GetLinkingEdge(v0, v2);
        const int e1 = GetLinkingEdge(v1, v2);
        if ((e0 >= 0 && edges_[e0].edge_type == kEdgeInner) || (e1 >= 0 && edges_[e1].edge_type == kEdgeInner)) {
            return {false, Vec3{}};
        }

        auto [valid, center] = ComputeBallCenter(v0, v1, v2, radius);
        if (!valid) {
            return {false, Vec3{}};
        }

        if (!IsEmptyBall(center, radius, v0, v1, v2, neighborhood)) {
            return {false, Vec3{}};
        }

        return {true, center};
    }

    bool TrySeed(int vertex_idx, double radius) {
        std::vector<int> indices;
        const int32_t start = row_ptr_[vertex_idx];
        const int32_t end = row_ptr_[vertex_idx + 1];
        indices.reserve(std::max<int32_t>(0, end - start));
        for (int32_t p = start; p < end; ++p) {
            const int nb = static_cast<int>(col_idx_[p]);
            if (nb >= 0 && nb < n_points_) {
                indices.push_back(nb);
            }
        }

        if (indices.size() < 2) {
            return false;
        }

        for (size_t i = 0; i < indices.size(); ++i) {
            const int nb0 = indices[i];
            if (nb0 == vertex_idx || vertex_types_[nb0] != kVertexOrphan) {
                continue;
            }

            int candidate_v2 = -1;
            Vec3 candidate_center{};

            for (size_t j = i + 1; j < indices.size(); ++j) {
                const int nb1 = indices[j];
                if (nb1 == vertex_idx || vertex_types_[nb1] != kVertexOrphan) {
                    continue;
                }

                auto [ok, center] = TryTriangleSeed(vertex_idx, nb0, nb1, radius, indices);
                if (ok) {
                    candidate_v2 = nb1;
                    candidate_center = center;
                    break;
                }
            }

            if (candidate_v2 < 0) {
                continue;
            }

            const int e0 = GetLinkingEdge(vertex_idx, candidate_v2);
            const int e1 = GetLinkingEdge(nb0, candidate_v2);
            const int e2 = GetLinkingEdge(vertex_idx, nb0);
            if ((e0 >= 0 && edges_[e0].edge_type != kEdgeFront) || (e1 >= 0 && edges_[e1].edge_type != kEdgeFront) ||
                (e2 >= 0 && edges_[e2].edge_type != kEdgeFront)) {
                continue;
            }

            if (!CreateTriangle(vertex_idx, nb0, candidate_v2, candidate_center)) {
                return false;
            }

            const int ne0 = GetLinkingEdge(vertex_idx, candidate_v2);
            const int ne1 = GetLinkingEdge(nb0, candidate_v2);
            const int ne2 = GetLinkingEdge(vertex_idx, nb0);

            const int seed_edges[3] = {ne0, ne1, ne2};
            for (int edge_idx : seed_edges) {
                if (edge_idx >= 0 && edges_[edge_idx].edge_type == kEdgeFront) {
                    edge_front_.push_front(edge_idx);
                }
            }

            if (!edge_front_.empty()) {
                return true;
            }
        }

        return false;
    }

    void ExpandTriangulation(double radius) {
        while (!edge_front_.empty()) {
            const int edge_idx = edge_front_.front();
            edge_front_.pop_front();

            if (edges_[edge_idx].edge_type != kEdgeFront) {
                continue;
            }

            const EdgeRecord edge_snapshot = edges_[edge_idx];
            const auto result = FindCandidateVertex(edge_idx, radius);

            if (!result.has_value() || result->candidate < 0 || vertex_types_[result->candidate] == kVertexInner ||
                !IsCompatible(result->candidate, edge_snapshot.source, edge_snapshot.target)) {
                edges_[edge_idx].edge_type = kEdgeBorder;
                border_edges_.push_back(edge_idx);
                continue;
            }

            const int e0 = GetLinkingEdge(result->candidate, edge_snapshot.source);
            const int e1 = GetLinkingEdge(result->candidate, edge_snapshot.target);
            if ((e0 >= 0 && edges_[e0].edge_type != kEdgeFront) || (e1 >= 0 && edges_[e1].edge_type != kEdgeFront)) {
                edges_[edge_idx].edge_type = kEdgeBorder;
                border_edges_.push_back(edge_idx);
                continue;
            }

            if (!CreateTriangle(edge_snapshot.source, edge_snapshot.target, result->candidate, result->center)) {
                return;
            }

            const int ne0 = GetLinkingEdge(result->candidate, edge_snapshot.source);
            const int ne1 = GetLinkingEdge(result->candidate, edge_snapshot.target);
            if (ne0 >= 0 && edges_[ne0].edge_type == kEdgeFront) {
                edge_front_.push_front(ne0);
            }
            if (ne1 >= 0 && edges_[ne1].edge_type == kEdgeFront) {
                edge_front_.push_front(ne1);
            }
        }
    }

    // Stage-2 front processing: process conflict-free batches to reduce strict seriality.
    void ExpandTriangulationBatched(double radius, int batch_size) {
        while (!edge_front_.empty()) {
            std::vector<int> active_edges;
            active_edges.reserve(static_cast<size_t>(batch_size));

            std::vector<int> deferred_edges;
            deferred_edges.reserve(static_cast<size_t>(batch_size));

            batch_vertex_tag_ += 1;
            if (batch_vertex_tag_ >= std::numeric_limits<int32_t>::max()) {
                std::fill(batch_vertex_mark_.begin(), batch_vertex_mark_.end(), 0);
                batch_vertex_tag_ = 1;
            }
            const int32_t batch_tag = batch_vertex_tag_;

            while (!edge_front_.empty() && static_cast<int>(active_edges.size()) < batch_size) {
                const int edge_idx = edge_front_.front();
                edge_front_.pop_front();

                if (edges_[edge_idx].edge_type != kEdgeFront) {
                    continue;
                }

                const EdgeRecord &edge = edges_[edge_idx];
                if (batch_vertex_mark_[edge.source] == batch_tag || batch_vertex_mark_[edge.target] == batch_tag) {
                    deferred_edges.push_back(edge_idx);
                    continue;
                }

                active_edges.push_back(edge_idx);
                batch_vertex_mark_[edge.source] = batch_tag;
                batch_vertex_mark_[edge.target] = batch_tag;
            }

            if (active_edges.empty()) {
                for (int edge_idx : deferred_edges) {
                    edge_front_.push_back(edge_idx);
                }
                continue;
            }

            std::vector<int> accepted_new_edges;
            accepted_new_edges.reserve(active_edges.size() * 2U);

            for (int edge_idx : active_edges) {
                if (edges_[edge_idx].edge_type != kEdgeFront) {
                    continue;
                }

                const EdgeRecord edge_snapshot = edges_[edge_idx];
                const auto result = FindCandidateVertex(edge_idx, radius);

                if (!result.has_value() || result->candidate < 0 || vertex_types_[result->candidate] == kVertexInner ||
                    !IsCompatible(result->candidate, edge_snapshot.source, edge_snapshot.target)) {
                    edges_[edge_idx].edge_type = kEdgeBorder;
                    border_edges_.push_back(edge_idx);
                    continue;
                }

                const int e0 = GetLinkingEdge(result->candidate, edge_snapshot.source);
                const int e1 = GetLinkingEdge(result->candidate, edge_snapshot.target);
                if ((e0 >= 0 && edges_[e0].edge_type != kEdgeFront) || (e1 >= 0 && edges_[e1].edge_type != kEdgeFront)) {
                    edges_[edge_idx].edge_type = kEdgeBorder;
                    border_edges_.push_back(edge_idx);
                    continue;
                }

                if (!CreateTriangle(edge_snapshot.source, edge_snapshot.target, result->candidate, result->center)) {
                    return;
                }
                batch_vertex_mark_[result->candidate] = batch_tag;

                const int ne0 = GetLinkingEdge(result->candidate, edge_snapshot.source);
                const int ne1 = GetLinkingEdge(result->candidate, edge_snapshot.target);
                if (ne0 >= 0 && edges_[ne0].edge_type == kEdgeFront) {
                    accepted_new_edges.push_back(ne0);
                }
                if (ne1 >= 0 && edges_[ne1].edge_type == kEdgeFront) {
                    accepted_new_edges.push_back(ne1);
                }
            }

            for (int edge_idx : accepted_new_edges) {
                edge_front_.push_front(edge_idx);
            }
            for (int edge_idx : deferred_edges) {
                edge_front_.push_back(edge_idx);
            }
        }
    }

    void ExpandTriangulationWithMode(double radius, const std::string &front_mode, int front_batch_size) {
        if (front_mode == "batched") {
            ExpandTriangulationBatched(radius, front_batch_size);
        } else {
            ExpandTriangulation(radius);
        }
    }

    void FindSeedTriangle(double radius, const std::string &front_mode, int front_batch_size) {
        for (int vertex_idx = 0; vertex_idx < n_points_; ++vertex_idx) {
            if (vertex_types_[vertex_idx] != kVertexOrphan) {
                continue;
            }

            if (TrySeed(vertex_idx, radius)) {
                ExpandTriangulationWithMode(radius, front_mode, front_batch_size);
            }

            if (max_triangles_ > 0 && static_cast<int>(faces_.size()) >= max_triangles_) {
                return;
            }
        }
    }

    void RefreshBorderEdgesForRadius(double radius) {
        std::vector<int> kept;
        kept.reserve(border_edges_.size());

        for (int edge_idx : border_edges_) {
            EdgeRecord &edge = edges_[edge_idx];
            if (edge.triangle0 < 0) {
                kept.push_back(edge_idx);
                continue;
            }

            const TriangleRecord &tri = triangles_[edge.triangle0];
            auto [valid, center] = ComputeBallCenter(tri.v0, tri.v1, tri.v2, radius);
            if (!valid) {
                kept.push_back(edge_idx);
                continue;
            }

            const Vec3 midpoint = (points_[tri.v0] + points_[tri.v1] + points_[tri.v2]) / 3.0;
            const std::vector<int> candidates = CandidatePoolFromVertices(tri.v0, tri.v1, tri.v2, midpoint, radius);

            if (IsEmptyBallExcludingThree(center, radius, tri.v0, tri.v1, tri.v2, candidates)) {
                edge.edge_type = kEdgeFront;
                edge_front_.push_back(edge_idx);
            } else {
                kept.push_back(edge_idx);
            }
        }

        border_edges_.swap(kept);
    }

private:
    std::vector<Vec3> points_;
    std::vector<Vec3> normals_;

    std::vector<int32_t> row_ptr_;
    std::vector<int32_t> col_idx_;

    int n_points_;
    int max_triangles_;

    std::vector<int8_t> vertex_types_;
    std::vector<std::unordered_set<int>> vertex_edges_;

    std::vector<EdgeRecord> edges_;
    std::unordered_map<uint64_t, int> edge_lookup_;

    std::vector<TriangleRecord> triangles_;
    std::vector<std::array<int, 3>> faces_;

    std::deque<int> edge_front_;
    std::vector<int> border_edges_;

    std::vector<int32_t> candidate_mark_;
    int32_t candidate_tag_;
    std::vector<int32_t> batch_vertex_mark_;
    int32_t batch_vertex_tag_;
};

// -----------------------------------------------------------------------------
// pybind11 Entry
// -----------------------------------------------------------------------------
py::array_t<int32_t> RunTopology(
    py::array_t<double, py::array::c_style | py::array::forcecast> points,
    py::array_t<double, py::array::c_style | py::array::forcecast> normals,
    py::array_t<int32_t, py::array::c_style | py::array::forcecast> row_ptr,
    py::array_t<int32_t, py::array::c_style | py::array::forcecast> col_idx,
    const std::vector<double> &radii,
    int max_triangles,
    const std::string &front_mode,
    int front_batch_size) {
    const auto p_buf = points.request();
    const auto n_buf = normals.request();
    const auto rp_buf = row_ptr.request();
    const auto ci_buf = col_idx.request();

    if (p_buf.ndim != 2 || p_buf.shape[1] != 3) {
        throw std::runtime_error("points must have shape (n_points, 3)");
    }
    if (n_buf.ndim != 2 || n_buf.shape[1] != 3) {
        throw std::runtime_error("normals must have shape (n_points, 3)");
    }
    if (p_buf.shape[0] != n_buf.shape[0]) {
        throw std::runtime_error("points and normals must have the same number of rows");
    }
    if (rp_buf.ndim != 1) {
        throw std::runtime_error("row_ptr must have rank 1");
    }
    if (ci_buf.ndim != 1) {
        throw std::runtime_error("col_idx must have rank 1");
    }

    const int n_points = static_cast<int>(p_buf.shape[0]);
    if (rp_buf.shape[0] != n_points + 1) {
        throw std::runtime_error("row_ptr must have shape (n_points + 1,)");
    }

    const auto *p_ptr = static_cast<const double *>(p_buf.ptr);
    const auto *n_ptr = static_cast<const double *>(n_buf.ptr);
    const auto *rp_ptr = static_cast<const int32_t *>(rp_buf.ptr);
    const auto *ci_ptr = static_cast<const int32_t *>(ci_buf.ptr);

    std::vector<Vec3> points_host;
    std::vector<Vec3> normals_host;
    points_host.reserve(static_cast<size_t>(n_points));
    normals_host.reserve(static_cast<size_t>(n_points));

    for (int i = 0; i < n_points; ++i) {
        points_host.emplace_back(p_ptr[3 * i + 0], p_ptr[3 * i + 1], p_ptr[3 * i + 2]);

        Vec3 n{n_ptr[3 * i + 0], n_ptr[3 * i + 1], n_ptr[3 * i + 2]};
        n = NormalizeOrZero(n);
        normals_host.emplace_back(n);
    }

    std::vector<int32_t> row_ptr_host(rp_ptr, rp_ptr + rp_buf.shape[0]);
    std::vector<int32_t> col_idx_host(ci_ptr, ci_ptr + ci_buf.shape[0]);

    BallPivotState state(
        std::move(points_host),
        std::move(normals_host),
        std::move(row_ptr_host),
        std::move(col_idx_host),
        max_triangles);

    std::vector<int32_t> faces = state.Run(radii, front_mode, front_batch_size);

    const ssize_t n_faces = static_cast<ssize_t>(faces.size() / 3U);
    py::array_t<int32_t> out({n_faces, static_cast<ssize_t>(3)});
    auto out_buf = out.mutable_unchecked<2>();
    for (ssize_t i = 0; i < n_faces; ++i) {
        out_buf(i, 0) = faces[3 * i + 0];
        out_buf(i, 1) = faces[3 * i + 1];
        out_buf(i, 2) = faces[3 * i + 2];
    }
    return out;
}

}  // namespace

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
    m.doc() = "Compiled topology core for point_cloud_ball_pivoting";

    m.def(
        "run_topology",
        &RunTopology,
        py::arg("points"),
        py::arg("normals"),
        py::arg("row_ptr"),
        py::arg("col_idx"),
        py::arg("radii"),
        py::arg("max_triangles"),
        py::arg("front_mode"),
        py::arg("front_batch_size"));
}
