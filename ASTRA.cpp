// A.S.T.R.A. — Asset Storage, Tiering, and Rapid Access
// Reference implementation (C++17). Single-file example.
//
// Compile: g++ -std=c++17 astra.cpp -O2 -pthread -o astra
//
// DISCLAIMER: placeholder IO and GPU code. Replace with platform-specific APIs.

#include <bits/stdc++.h>
using namespace std;
using chrono_ms = chrono::milliseconds;
using Clock = chrono::steady_clock;

// ----------------------------- Configuration ------------------------------

namespace ASTRAConfig {
    constexpr size_t RAM_BUDGET_BYTES = 200 * 1024 * 1024;   // 200 MB RAM budget for decoded assets
    constexpr size_t VRAM_BUDGET_BYTES = 120 * 1024 * 1024;  // 120 MB VRAM budget
    constexpr int STREAM_THREAD_COUNT = 4;
    constexpr int PRIORITY_LEVELS = 4; // 0 (highest) .. 3 (lowest)
    constexpr chrono_ms DECISION_TICK = chrono_ms(100); // scheduler tick
    constexpr size_t CHUNK_SIZE = 64 * 1024; // 64 KB per chunk default
    constexpr int PREFETCH_HORIZON = 3; // how many chunks ahead to prefetch by default
}

// ------------------------------ Utilities ---------------------------------

static inline string nowStr() {
    auto t = chrono::system_clock::now();
    time_t tt = chrono::system_clock::to_time_t(t);
    char buf[64];
    strftime(buf, sizeof(buf), "%F %T", localtime(&tt));
    return string(buf);
}

template<typename T>
string to_hex(T v) {
    std::ostringstream oss;
    oss << hex << v;
    return oss.str();
}

// ------------------------------ Types -------------------------------------

using AssetID = uint64_t;
using ChunkID = uint64_t;
using Priority = int; // 0 highest ... N low

enum class AssetState {
    OnDisk,
    Streaming,
    InRAM,
    InVRAM,
    Evicted,
    Error
};

struct ChunkData {
    vector<uint8_t> bytes;
    // checksum for integrity checks
    uint64_t checksum = 0;

    ChunkData() = default;
    explicit ChunkData(size_t size) : bytes(size) {
        // fill zero for deterministic behavior in demo
        fill(bytes.begin(), bytes.end(), 0);
        checksum = 0;
    }
};

struct AssetMeta {
    AssetID id;
    string name;
    size_t fullSizeBytes;
    int chunkCount;
    // optional hints
    bool hero = false; // never evict without manual override
    Priority priority = ASTRAConfig::PRIORITY_LEVELS - 1;
    // perceptual importance (0..1)
    float perceptualImportance = 0.5f;
};

// ------------------------- Simulated Disk Storage ---------------------------
// In real engine, replace with file IO, package systems, streaming server, CDN, etc.

class DiskSimulator {
public:
    DiskSimulator() = default;

    // store chunk associated with (assetID, chunkIndex)
    void storeChunk(AssetID aid, int chunkIndex, const ChunkData &data) {
        std::unique_lock<std::mutex> lock(mutex_);
        disk_[makeKey(aid, chunkIndex)] = data;
    }

    // synchronous read (simulate latency)
    bool readChunk(AssetID aid, int chunkIndex, ChunkData &out) {
        // simulate disk latency
        std::this_thread::sleep_for(chrono_ms(10 + (rand() % 20)));
        std::unique_lock<std::mutex> lock(mutex_);
        auto it = disk_.find(makeKey(aid, chunkIndex));
        if (it == disk_.end()) return false;
        out = it->second;
        return true;
    }

    bool hasChunk(AssetID aid, int chunkIndex) {
        std::unique_lock<std::mutex> lock(mutex_);
        return disk_.count(makeKey(aid, chunkIndex))>0;
    }

private:
    string makeKey(AssetID a, int c) const {
        return to_string(a) + ":" + to_string(c);
    }
    unordered_map<string, ChunkData> disk_;
    mutable mutex mutex_;
};

// --------------------------- Asset & Package --------------------------------

struct AssetPackage {
    AssetMeta meta;
    // chunk indices are 0..chunkCount-1, stored on disk simulator
};

struct AssetResidency {
    AssetID id;
    AssetState state = AssetState::OnDisk;
    size_t residentRAMBytes = 0;
    size_t residentVRAMBytes = 0;
    Clock::time_point lastAccess = Clock::now();
    int inRAMChunks = 0;
    int inVRAMChunks = 0;
    bool pinned = false; // prevents eviction
};

// --------------------------- LRU + Priority Cache ---------------------------

template<typename Key, typename Value>
class LRUCache {
public:
    LRUCache(size_t capacityBytes = ASTRAConfig::RAM_BUDGET_BYTES)
        : capacityBytes_(capacityBytes), usedBytes_(0) {}

    void put(const Key &k, Value v, size_t bytes) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (map_.count(k)) {
            // update: move to front
            auto it = map_[k];
            usedBytes_ -= it->bytes;
            list_.erase(it->it);
            map_.erase(k);
        }
        list_.push_front(k);
        map_[k] = { v, bytes, list_.begin() };
        usedBytes_ += bytes;
        shrinkIfNeeded();
    }

    bool get(const Key &k, Value &out) {
        std::unique_lock<std::mutex> lock(mutex_);
        auto it = map_.find(k);
        if (it == map_.end()) return false;
        // move to front
        list_.erase(it->second.it);
        list_.push_front(k);
        it->second.it = list_.begin();
        out = it->second.value;
        return true;
    }

    bool contains(const Key &k) {
        std::unique_lock<std::mutex> lock(mutex_);
        return map_.count(k) > 0;
    }

    // evict by LRU policy until usage <= capacity
    vector<Key> evictLRU(size_t targetBytes) {
        vector<Key> evicted;
        std::unique_lock<std::mutex> lock(mutex_);
        while (usedBytes_ > targetBytes && !list_.empty()) {
            Key k = list_.back();
            auto it = map_.find(k);
            if (it == map_.end()) break;
            usedBytes_ -= it->second.bytes;
            evicted.push_back(k);
            map_.erase(it);
            list_.pop_back();
        }
        return evicted;
    }

    size_t usedBytes() const { return usedBytes_; }
    size_t capacityBytes() const { return capacityBytes_; }
    void setCapacity(size_t b) {
        std::unique_lock<std::mutex> lock(mutex_);
        capacityBytes_ = b;
    }

    // For demo/testing
    vector<Key> keysSnapshot() {
        std::unique_lock<std::mutex> lock(mutex_);
        vector<Key> k;
        k.reserve(list_.size());
        for (auto &it : list_) k.push_back(it);
        return k;
    }

private:
    struct Entry {
        Value value;
        size_t bytes;
        typename list<Key>::iterator it;
    };

    void shrinkIfNeeded() {
        while (usedBytes_ > capacityBytes_ && !list_.empty()) {
            Key k = list_.back();
            auto it = map_.find(k);
            if (it == map_.end()) break;
            usedBytes_ -= it->second.bytes;
            map_.erase(it);
            list_.pop_back();
        }
    }

    mutable mutex mutex_;
    unordered_map<Key, Entry> map_;
    list<Key> list_;
    size_t capacityBytes_;
    size_t usedBytes_;
};

// --------------------------- Thread Pool ------------------------------------

class ThreadPool {
public:
    ThreadPool(size_t n) : stop_(false) {
        for (size_t i = 0; i < n; ++i) {
            workers_.emplace_back([this]() {
                while (true) {
                    function<void()> task;
                    {
                        unique_lock<mutex> lock(queueMutex_);
                        cv_.wait(lock, [this]() { return stop_ || !tasks_.empty(); });
                        if (stop_ && tasks_.empty()) return;
                        task = std::move(tasks_.front());
                        tasks_.pop_front();
                    }
                    try { task(); } catch (...) { /* swallow */ }
                }
            });
        }
    }

    ~ThreadPool() {
        {
            unique_lock<mutex> lock(queueMutex_);
            stop_ = true;
        }
        cv_.notify_all();
        for (auto &w : workers_) if (w.joinable()) w.join();
    }

    template<typename F>
    void enqueue(F f) {
        {
            unique_lock<mutex> lock(queueMutex_);
            tasks_.push_back(function<void()>(f));
        }
        cv_.notify_one();
    }

private:
    vector<thread> workers_;
    deque<function<void()>> tasks_;
    mutex queueMutex_;
    condition_variable cv_;
    bool stop_;
};

// --------------------------- ASTRA Core ------------------------------------

class ASTRA {
public:
    ASTRA(DiskSimulator &disk)
        : disk_(disk),
          ramCache_(ASTRAConfig::RAM_BUDGET_BYTES),
          vramCache_(ASTRAConfig::VRAM_BUDGET_BYTES),
          pool_(ASTRAConfig::STREAM_THREAD_COUNT),
          running_(true)
    {
        // launch decision loop
        plannerThread_ = thread([this]() { plannerLoop(); });
    }

    ~ASTRA() {
        running_ = false;
        if (plannerThread_.joinable()) plannerThread_.join();
    }

    // register package (metadata) and ensure disk contains chunks
    void registerPackage(const AssetPackage &pkg, const vector<ChunkData> &chunksOnDisk) {
        unique_lock<mutex> lock(metaMutex_);
        metaMap_[pkg.meta.id] = pkg;
        // write to disk simulator
        for (int i = 0; i < (int)chunksOnDisk.size(); ++i) {
            disk_.storeChunk(pkg.meta.id, i, chunksOnDisk[i]);
        }
        // create residency entry
        AssetResidency r;
        r.id = pkg.meta.id;
        r.state = AssetState::OnDisk;
        residency_[pkg.meta.id] = r;
    }

    // Request an asset to be resident in VRAM (highest)
    // This returns immediately; the streaming will happen asynchronously.
    void requestAssetForVRAM(AssetID id, Priority p = 1) {
        // register a request ticket
        {
            unique_lock<mutex> lock(reqMutex_);
            RequestTicket t;
            t.assetId = id;
            t.priority = p;
            t.requestTime = Clock::now();
            t.target = AssetState::InVRAM;
            requestQueue_.push_back(move(t));
        }
        reqCv_.notify_one();
    }

    // Request asset for RAM (decoded but not uploaded to GPU)
    void requestAssetForRAM(AssetID id, Priority p = 2) {
        unique_lock<mutex> lock(reqMutex_);
        RequestTicket t;
        t.assetId = id;
        t.priority = p;
        t.requestTime = Clock::now();
        t.target = AssetState::InRAM;
        requestQueue_.push_back(move(t));
        reqCv_.notify_one();
    }

    // release (unpin) an asset (allow eviction)
    void releaseAsset(AssetID id) {
        unique_lock<mutex> lock(metaMutex_);
        if (residency_.count(id)) {
            residency_[id].pinned = false;
        }
    }

    // pin an asset to avoid eviction
    void pinAsset(AssetID id) {
        unique_lock<mutex> lock(metaMutex_);
        if (residency_.count(id)) {
            residency_[id].pinned = true;
        }
    }

    // get snapshot of status (for debug)
    vector<pair<AssetID, AssetState>> snapshotStatus() {
        vector<pair<AssetID, AssetState>> out;
        unique_lock<mutex> lock(metaMutex_);
        for (auto &kv : residency_) {
            out.push_back({kv.first, kv.second.state});
        }
        return out;
    }

    // Hook for external predictor (QNEP / N.O.V.A.) to enqueue prefetches
    void prefetchTiles(const vector<AssetID> &ids, Priority p = 2) {
        unique_lock<mutex> lock(reqMutex_);
        for (auto id : ids) {
            requestQueue_.push_back({id, p, Clock::now(), AssetState::InRAM});
        }
        reqCv_.notify_one();
    }

    // diagnostic: wait until no active streaming operations (for tests)
    void waitForIdle() {
        unique_lock<mutex> lock(activeMutex_);
        activeCv_.wait(lock, [this]() { return activeCount_ == 0 && requestQueue_.empty(); });
    }

private:
    // internal types
    struct RequestTicket {
        AssetID assetId;
        Priority priority;
        Clock::time_point requestTime;
        AssetState target;
    };

    // internal helper: schedule chunk load jobs
    void scheduleChunkLoad(AssetID aid, int chunkIndex, Priority prio, AssetState target) {
        // The thread pool executes disk reads, decode, and optional GPU upload
        incrementActive();
        pool_.enqueue([this, aid, chunkIndex, prio, target]() {
            // read from disk
            ChunkData chunk;
            bool ok = disk_.readChunk(aid, chunkIndex, chunk);
            if (!ok) {
                LOG("Disk miss for asset " << aid << " chunk " << chunkIndex);
                decrementActive();
                onError(aid);
                return;
            }
            // decode (simulate)
            decodeChunkInRAM(aid, chunkIndex, chunk);

            if (target == AssetState::InVRAM) {
                // optional GPU upload (simulate)
                uploadChunkToVRAM(aid, chunkIndex, chunk);
            }

            // mark residency access
            touchResidency(aid, chunk.bytes.size(), target);
            decrementActive();
        });
    }

    void decodeChunkInRAM(AssetID aid, int chunkIndex, const ChunkData &chunk) {
        // simulate CPU decode cost (e.g., decompress, parse)
        std::this_thread::sleep_for(chrono_ms(5 + (rand() % 5)));
        string key = ramKey(aid, chunkIndex);
        // store in RAM cache (key -> data) with chunk bytes size
        ramCache_.put(key, chunk, chunk.bytes.size());
        // update residency
        {
            unique_lock<mutex> lock(metaMutex_);
            auto &r = residency_[aid];
            r.state = AssetState::InRAM;
            r.lastAccess = Clock::now();
            r.inRAMChunks++;
            r.residentRAMBytes += chunk.bytes.size();
        }
    }

    void uploadChunkToVRAM(AssetID aid, int chunkIndex, const ChunkData &chunk) {
        // simulate GPU upload cost and VRAM allocation
        std::this_thread::sleep_for(chrono_ms(3 + (rand() % 4)));
        string key = vramKey(aid, chunkIndex);
        // store in VRAM cache
        vramCache_.put(key, chunk, chunk.bytes.size()); // use same chunk size measure
        {
            unique_lock<mutex> lock(metaMutex_);
            auto &r = residency_[aid];
            r.state = AssetState::InVRAM;
            r.lastAccess = Clock::now();
            r.inVRAMChunks++;
            r.residentVRAMBytes += chunk.bytes.size();
        }
    }

    // planner loop picks requests, forms plans, enqueues chunk loads, and triggers evictions when budgets breached
    void plannerLoop() {
        while (running_) {
            // process incoming requests
            vector<RequestTicket> tickets;
            {
                unique_lock<mutex> lock(reqMutex_);
                if (requestQueue_.empty()) {
                    // wait for next tick or new requests
                    reqCv_.wait_for(lock, ASTRAConfig::DECISION_TICK);
                }
                // drain queue
                while (!requestQueue_.empty()) {
                    tickets.push_back(requestQueue_.front());
                    requestQueue_.pop_front();
                }
            }
            if (!tickets.empty()) {
                // sort by priority + recency heuristic
                sort(tickets.begin(), tickets.end(), [](const RequestTicket &a, const RequestTicket &b) {
                    if (a.priority != b.priority) return a.priority < b.priority;
                    return a.requestTime < b.requestTime;
                });

                // For each ticket build streaming plan (figure chunks, etc.)
                for (auto &t : tickets) {
                    handleTicket(t);
                }
            }
            // Check budgets and evict if necessary
            manageBudgets();

            // notify idle if done
            {
                unique_lock<mutex> lock(activeMutex_);
                if (activeCount_ == 0 && requestQueue_.empty()) activeCv_.notify_all();
            }
            // tick sleep minimal
            std::this_thread::sleep_for(chrono_ms(1));
        }
    }

    void handleTicket(const RequestTicket &t) {
        // sanity: does meta exist?
        AssetMeta meta;
        {
            unique_lock<mutex> lock(metaMutex_);
            if (metaMap_.count(t.assetId) == 0) {
                LOG("Request for unknown asset " << t.assetId);
                return;
            }
            meta = metaMap_[t.assetId].meta;
        }

        // if hero asset and pinned, honor immediately
        if (meta.hero) pinAsset(meta.id);

        // decide chunk strategy: simple: stream all chunks, but can be optimized
        int chunks = meta.chunkCount;
        // prefer sequential streaming order; may use predictive ordering (based on camera, etc.)
        for (int c = 0; c < chunks; ++c) {
            // check if already in RAM/VRAM
            if (t.target == AssetState::InVRAM) {
                // check VRAM cache
                if (vramCache_.contains(vramKey(meta.id, c))) {
                    // already resident, update metadata
                    touchResidency(meta.id, 0, AssetState::InVRAM);
                    continue;
                }
            }
            if (t.target == AssetState::InRAM) {
                if (ramCache_.contains(ramKey(meta.id, c))) { touchResidency(meta.id, 0, AssetState::InRAM); continue; }
            }

            // decide prioritized chunk order with small prefetch horizon
            int start = c;
            int horizon = min(ASTRAConfig::PREFETCH_HORIZON, chunks - start);
            for (int offset = 0; offset < horizon; ++offset) {
                int ci = start + offset;
                // queue chunk load job
                scheduleChunkLoad(meta.id, ci, t.priority, t.target);
            }
            // advance c by horizon (we will still loop but these chunks will already be requested)
            c += (horizon - 1);
        }
    }

    void manageBudgets() {
        // compute used and evict if needed
        size_t ramUsed = ramCache_.usedBytes();
        size_t ramCap = ramCache_.capacityBytes();
        if (ramUsed > ramCap) {
            // evict LRU keys until within target (e.g., 90% of capacity)
            size_t target = ramCap * 9 / 10;
            auto evictedKeys = ramCache_.evictLRU(target);
            for (auto &k : evictedKeys) {
                // parse key to identify asset and chunk
                auto aid = parseKeyToAsset(k);
                int chunkIndex = parseKeyToChunk(k);
                unique_lock<mutex> lock(metaMutex_);
                if (residency_.count(aid)) {
                    residency_[aid].inRAMChunks = max(0, residency_[aid].inRAMChunks - 1);
                    // reduce accounting bytes conservatively
                    // In real engine we'd track actual bytes per chunk per asset
                }
            }
        }
        size_t vramUsed = vramCache_.usedBytes();
        size_t vramCap = vramCache_.capacityBytes();
        if (vramUsed > vramCap) {
            size_t target = vramCap * 9 / 10;
            auto evictedKeys = vramCache_.evictLRU(target);
            for (auto &k : evictedKeys) {
                auto aid = parseKeyToAsset(k);
                int chunkIndex = parseKeyToChunk(k);
                unique_lock<mutex> lock(metaMutex_);
                if (residency_.count(aid)) {
                    residency_[aid].inVRAMChunks = max(0, residency_[aid].inVRAMChunks - 1);
                }
            }
        }
    }

    // logging macro
    #define LOG(x) do { cerr << "[" << nowStr() << "] [ASTRA] " << x << endl; } while(0)

    void onError(AssetID aid) {
        unique_lock<mutex> lock(metaMutex_);
        residency_[aid].state = AssetState::Error;
        // notify NERVA/BIS hooks here (omitted for simplicity)
    }

    void touchResidency(AssetID aid, size_t bytes, AssetState state) {
        unique_lock<mutex> lock(metaMutex_);
        if (residency_.count(aid) == 0) return;
        auto &r = residency_[aid];
        r.lastAccess = Clock::now();
        if (state == AssetState::InRAM) {
            r.inRAMChunks++;
            r.residentRAMBytes += bytes;
            r.state = AssetState::InRAM;
        } else if (state == AssetState::InVRAM) {
            r.inVRAMChunks++;
            r.residentVRAMBytes += bytes;
            r.state = AssetState::InVRAM;
        }
    }

    void incrementActive() {
        unique_lock<mutex> lock(activeMutex_);
        activeCount_++;
    }
    void decrementActive() {
        unique_lock<mutex> lock(activeMutex_);
        activeCount_--;
        if (activeCount_ == 0 && requestQueue_.empty()) activeCv_.notify_all();
    }

    // helpers for keys
    string ramKey(AssetID a, int chunk) const { return "RAM:" + to_string(a) + ":" + to_string(chunk); }
    string vramKey(AssetID a, int chunk) const { return "VRAM:" + to_string(a) + ":" + to_string(chunk); }
    AssetID parseKeyToAsset(const string &key) {
        // simple parse "RAM:aid:chunk" or "VRAM:aid:chunk"
        vector<string> parts;
        string tmp;
        for (char c : key) {
            if (c == ':') { parts.push_back(tmp); tmp.clear(); } else tmp.push_back(c);
        }
        parts.push_back(tmp);
        if (parts.size() >= 3) return stoull(parts[1]);
        return 0;
    }
    int parseKeyToChunk(const string &key) {
        vector<string> parts;
        string tmp;
        for (char c : key) {
            if (c == ':') { parts.push_back(tmp); tmp.clear(); } else tmp.push_back(c);
        }
        parts.push_back(tmp);
        if (parts.size() >= 3) return stoi(parts[2]);
        return 0;
    }

    // members
    DiskSimulator &disk_;
    unordered_map<AssetID, AssetPackage> metaMap_;
    unordered_map<AssetID, AssetResidency> residency_;
    mutex metaMutex_;

    LRUCache<string, ChunkData> ramCache_;
    LRUCache<string, ChunkData> vramCache_;

    ThreadPool pool_;

    // request queue
    deque<RequestTicket> requestQueue_;
    mutex reqMutex_;
    condition_variable reqCv_;

    // active operations counter
    mutex activeMutex_;
    condition_variable activeCv_;
    int activeCount_ = 0;

    // planner thread
    thread plannerThread_;
    atomic<bool> running_;

    // misc
    mutex ioMutex_;
};

// ----------------------------- Demo / Test ----------------------------------

int main() {
    srand((unsigned)time(nullptr));
    DiskSimulator disk;

    // create ASTRA
    ASTRA astra(disk);

    // Create and register several packages
    const int ASSET_COUNT = 8;
    for (int i = 1; i <= ASSET_COUNT; ++i) {
        AssetMeta m;
        m.id = i;
        m.name = "Asset_" + to_string(i);
        m.fullSizeBytes = 1024 * 1024; // pretend 1MB
        m.chunkCount = 16;
        m.hero = (i==1); // asset 1 is hero: rarely evict
        m.priority = (i==1) ? 0 : (i%3);
        AssetPackage pkg;
        pkg.meta = m;

        // prepare chunk data and write to disk simulator
        vector<ChunkData> chunks;
        for (int c = 0; c < m.chunkCount; ++c) {
            // create dummy chunk of CHUNK_SIZE
            ChunkData cd(ASTRAConfig::CHUNK_SIZE);
            // fill with some pattern to vary checksum
            for (size_t b = 0; b < cd.bytes.size(); ++b) cd.bytes[b] = (uint8_t)((i + c + b) & 0xFF);
            // simple checksum:
            uint64_t cs = 1469598103934665603ull;
            for (auto by : cd.bytes) cs = (cs ^ by) * 1099511628211ull;
            cd.checksum = cs;
            chunks.push_back(cd);
        }

        astra.registerPackage(pkg, chunks);
    }

    // Example usage: request some assets for VRAM & RAM
    cerr << "Requesting assets for VRAM and RAM..." << endl;
    astra.requestAssetForVRAM(1, 0); // hero asset
    astra.requestAssetForVRAM(2, 1);
    astra.requestAssetForRAM(3, 2);
    astra.requestAssetForRAM(4, 2);

    // Prefetch predicted next assets (simulate world predictor)
    vector<AssetID> predicted = {5, 6, 7};
    astra.prefetchTiles(predicted, 2);

    // Wait until done
    astra.waitForIdle();

    // Show snapshot
    auto snap = astra.snapshotStatus();
    cerr << "ASTRA Residency snapshot:" << endl;
    for (auto &p : snap) {
        cerr << "  Asset " << p.first << " state ";
        switch (p.second) {
            case AssetState::OnDisk: cerr << "OnDisk"; break;
            case AssetState::Streaming: cerr << "Streaming"; break;
            case AssetState::InRAM: cerr << "InRAM"; break;
            case AssetState::InVRAM: cerr << "InVRAM"; break;
            case AssetState::Evicted: cerr << "Evicted"; break;
            case AssetState::Error: cerr << "Error"; break;
        }
        cerr << endl;
    }

    cerr << "Demo finished." << endl;
    return 0;
}