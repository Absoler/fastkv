#include "kvstore.h"
#include "utils.h"
#include <cassert>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <string>
using namespace std;

/* ******************** my utils ******************** */
bool useBloomFilter = false;
bool useCache = true;
size_t maxSSEntry;
u64 gtimestamp; // sequence number of SSTable
string datadir;

// 判断intervals中记录的区间是否和range相交
bool in_intervals(const vector<pair<u64, u64>> &intervals, pair<u64, u64> range) {
    for (auto pir : intervals) {
        if (pir.first <= range.first && range.first <= pir.second || pir.first <= range.second && range.second <= pir.second) {
            return true;
        }
    }
    return false;
}

// 两个工具函数，用来从字节流中读写一个整数，例如十进制数256，在写成十六进制（小端模式下）就是 0x00, 0x01，前面一个字节是低位字节
// little endian load
u64 loadu(u8 *bytes, int n) {
    u64 val = 0;
    for (int i = 0; i < n; i++) {
        val = (val << 8) + bytes[n - i - 1];
    }
    return val;
}

// store in little endian
void storeu(u8 *bytes, u64 value, int n) {
    for (int i = 0; i < n; i++) {
        bytes[i] = value & 0xff;
        value = (value >> 8);
    }
}

bool
compare_ssentry(const SSEntry &sent1, const SSEntry &sent2) {
    return sent1.key < sent2.key;
}

typedef pair<SSEntry*, SSTable*> Sent_pir;
bool
compare_sent_pir(const Sent_pir &p1, const Sent_pir &p2) {
    if (p1.first->key != p2.first->key)
        return p1.first->key > p2.first->key;
    else
        return p1.second->timestamp < p2.second->timestamp;
}
// read a sstable header from given filename
// 读一个SSTable的header出来到head变量中，header的定义参照pdf
int
readSSTheader(const std::string &filename, SSTableHead &head) {
    int fd;
    u8 bytes[32];

    fd = open(filename.c_str(), O_RDONLY);
    read(fd, bytes, 32);
    head.timestamp = loadu(bytes, 8);
    head.cnt = loadu(bytes + 8, 8);
    head.minkey = loadu(bytes + 16, 8);
    head.maxkey = loadu(bytes + 24, 8);
    close(fd);
    return 1;
}

inline bool existLevelDir(const std::string &dir, int level) {
    string subdir = dir + "/level-" + to_string(level) + "/";
    return utils::dirExists(move(subdir));
}

// get sstable filenames from given level, if no .sst files, return 1
// sstable文件是分层存放的，这个函数用来读取给定层的ssttable文件路径
int getsstfiles(const string &dir, int level, list<string> &sstfiles) {
    vector<string> files;
    string subdir = dir + "/level-" + to_string(level) + "/";

    sstfiles.clear();
    if (!utils::dirExists(subdir)) {
        return 1;
    }
    utils::scanDir(subdir, files);
    for (string file : files) {
        size_t len = file.length();
        if (len >= 4 && file.substr(len-4).compare(".sst") == 0) {
            sstfiles.push_back(subdir + file);
        }
    }
    return (sstfiles.size() ? 0 : 1);
}

// select old sstfiles more than 2^(level+1), return false if no need
// if over limit, level-0 should all be merged
// 选出给定层有哪些文件是多出来需要合并到下一层的（优先淘汰时间戳小的）。
bool selectolds(std::list<std::string> &sstfiles, int level) {
    int more = sstfiles.size() - (2<<level);
    SSTableHead head;
    map<u64, vector<string>> timemap;

    if (level == 0) {
        return true;
    }

    if (more <= 0) {
        return false;
    }

    // 记录下每个文件的时间戳，然后排序，优先剔除选出小的
    for (string filename : sstfiles) {
        readSSTheader(filename, head);
        if (timemap.find(head.timestamp) == timemap.end()) {
            timemap[head.timestamp] = vector<string> ();
        }
        timemap[head.timestamp].push_back(filename);
    }

    sstfiles.clear();
    for (auto pir : timemap) {
        for (string filename: pir.second) {
            sstfiles.push_back(filename);
            more --;
            if (more <= 0) {
                break;
            }
        }
        if (more <= 0) {
            break;
        }
    }
    return true;
}

/* ******************** MemTable ******************** */
MemTable::MemTable() {
    head = new MemEntry(-1, -1, -1);
    maxLevel = 1;
    dis = std::uniform_real_distribution<double>(0.0, 1.0);
    size = 0;
}

MemTable::~MemTable() {
    MemEntry *cur = head, *next;
    while (cur) {
        next = cur->forward[0];
        delete cur;
        cur = next;
    }
}


// the next of the closet less element
// 从跳表中选出不大于key的元素
MemEntry *
MemTable::search_noless(u64 key) {
    MemEntry *cur = this->head;

    for (int i = maxLevel; i >= 0; i--) {
        if (!cur->forward[i])
            continue;
        while (cur->forward[i] && cur->forward[i]->sent.key < key) {
            cur = cur->forward[i];
        }
    }
    return cur->forward[0];
}

// return when the key exists, may be dead
KEY_STATE
MemTable::search(u64 key, MemEntry * &ment) {
    MemEntry *cur = search_noless(key);
    
    if (cur && cur->sent.key == key) {
        ment = cur;
        if (ment->dead) {
            return DEAD;
        }
        return LIVE;
    }
    return NOT_FOUND;
}

MemEntry *
MemTable::add(u64 key) {
    std::vector<MemEntry *> updates(MAX_SKIPLIST_LEVEL, head);
    MemEntry *cur = this->head;
    for (int i = maxLevel - 1; i >= 0; i--) {
        while (cur->forward[i] && cur->forward[i]->sent.key < key) {
        cur = cur->forward[i];
        }
        updates[i] = cur;
    }
    // level of the new node
    int lv = randomLv();
    maxLevel = std::max(lv, maxLevel);
    // the new node
    MemEntry *newnode = new MemEntry(key, -1, -1, false, lv);
    for (int i = 0; i < lv; i++) {
        
        newnode->forward[i] = updates[i]->forward[i];
        updates[i]->forward[i] = newnode;
    }
    size ++;
    return newnode;
}

int MemTable::randomLv() {
    int lv = 1;
    while(dis(gen) <= SKIPLIST_P && lv < MAX_SKIPLIST_LEVEL){
        lv ++;
    }
    return lv;
}

// try delete a key, return true if it exists
KEY_STATE
MemTable::erase(u64 key) {
    MemEntry *ent;
    KEY_STATE state;

    // 删除节点就是在跳表中选出他然后标记为dead
    state = search(key, ent);
    if (state == NOT_FOUND) {
        // 如果没有这个key，就插入一个新的
        // not find live `key`
        ent = add(key);
        // not add "~delete"
    }
    if (state != DEAD) {
        ent->dead = true;
        ent->sent.vlen = 0; // vlen == 0 means this entry is deleted
    }
    return state;
}

bool
MemTable::full() {
    return (!useCache) || size >= maxSSEntry;
}

/* ******************** SSTable ******************** */
void
SSTable::updateKey(u64 key) {
    // 更新当前sstable的最大最小key，用来get的时候查
    head.minkey = min(key, head.minkey);
    head.maxkey = max(key, head.maxkey);
}
// 按照filename的内容把对应文件加载进内存
void
SSTable::loadsents() {
    int fd;

    fd = open(filename.c_str(), O_RDONLY);
    lseek(fd, sizeof(SSTableHead), SEEK_SET);
    slen = sizeof(SSTableHead) + head.cnt * sizeof(SSEntry);
    data = new u8[slen];
    memcpy(data, &head, sizeof(SSTableHead));
    read(fd, data + sizeof(SSTableHead), slen - sizeof(SSTableHead));
    close(fd);
}

SSTable::SSTable(const string &filename, int level, bool needloadsents = true) {
    this->filename = filename;
    this->level = level;
    data = nullptr;

    head.cnt = 0;
    head.minkey = -1ULL;
    head.maxkey = 0;

    this->timestamp = gtimestamp;
    gtimestamp++;
    if (needloadsents) {
        readSSTheader(filename, head);
        loadsents();
        curp = get_content();
    }
}

SSTable::~SSTable() {
    delete data;
}

int
SSTable::save() {
    int fd;
    
    if (!utils::dirExists(datadir + "/level-" + to_string(level))) {
        utils::mkdir(datadir + "/level-" + to_string(level) + "/");
    }
    fd = open(filename.c_str(), O_WRONLY | O_CREAT, 0644);
    write(fd, data, slen);
    close(fd);
    return 0;
}

// set sstable's content from given input, in persistence
int SSTable::setdata(std::vector<SSEntry> &sent_vec, size_t start, size_t end) {
    SSEntry *sent;
    int fd;

    head = (SSTableHead){timestamp, end - start, sent_vec[start].key,
                        sent_vec[end - 1].key};
    slen = sizeof(SSEntry) * (end - start) + sizeof(SSTableHead);
    data = new u8[slen];
    memcpy(data, &head, sizeof(SSTableHead));
    sent = get_content();
    for (size_t i = start; i < end; i++) {
        sent[i - start] = sent_vec[i];
        updateKey(sent_vec[i].key);
    }
    curp = get_content();

    save();
    return 0;
}

// may find the wrong entry (key not equal), need recheck
// 从sstable加载进内存的内容中找到给定key的ssentry
SSEntry *
SSTable::find(u64 key) {
    if (!(head.minkey <= key && key <= head.maxkey)) {
        cerr << "try to get a key not in range" << endl;
        return get_content();
    }
    int lo, hi, mid;
    SSEntry *sents;

    // binary search in sstable
    lo = 0, hi = head.cnt;
    sents = get_content();
    while (lo < hi) {
        mid = (lo + hi) >> 1;
        if (sents[mid].key < key) {
            lo = mid + 1;
        } else if (sents[mid].key > key) {
            hi = mid;
        } else {
            break;
        }
    }
    return sents + mid;
}

SSEntry *
SSTable::get_content() {
    return (SSEntry *)(data + sizeof(SSTableHead));
}

bool
SSTable::full() {
    return head.cnt >= maxSSEntry;
}

// convert data from memtable into bytes
void
SSTable::convert(MemTable *memtable) {
    if (!memtable) {
        cerr << "no valid memtable to convert" << endl;
        data = nullptr;
        return;
    }
    MemEntry *ment;
    int cnt = 0;

    if (data)
        delete data;
    slen = sizeof(SSTableHead) + memtable->size * sizeof(SSEntry);
    data = new u8[slen];

    // write bloom filter

    // write data
    ment = memtable->head->forward[0];
    while (ment) {
        memcpy((u8 *)get_content() + head.cnt * sizeof(SSEntry), &ment->sent, sizeof(SSEntry));
        updateKey(ment->sent.key);
        ment = ment->forward[0];
        head.cnt += 1;
    }
    // write header
    memcpy(data, &head, sizeof(SSTableHead));
    curp = get_content();
}

// self-implemented iterator
SSEntry*
SSTable::get_next() {
    if (curp >= get_content() + head.cnt) {
        return nullptr;
    }
    SSEntry *res = curp;
    curp += 1;
    return res;
}

/* ******************** VLog ******************** */
size_t
VEntry::size() const {
    return value.length() + 15;
}

VLog::~VLog() {
    utils::rmfile(filename);
}

bool
VLog::createfile() {
    int fd;

    fd = open(filename.c_str(), O_WRONLY | O_CREAT, 0644);
    if (fd == -1) {
        cerr << "can't create VLog file" << endl;
        return false;
    }
    close(fd);
    return true;
}
// 在vlog文件后附加一个新的value
u64
VLog::append(u64 key, const std::string &value) {
    if (!utils::dirExists(filename)) {
        bool hasfile = createfile();
        if (!hasfile) {
            cerr << "can't append VLog file" << endl;
            return -1;
        }
    }

    vector<unsigned char> content(12, 0);	// for checksum
    u8 *bytes;
    u16 checksum;
    int fd, len;
    u64 offset;
    // 这里存数据的格式遵循pdf上的ventry
	for (int i = 0; i < 8; i++) {
		content[i] = 0xff & (key >> (i*8));
	}
    for (int i = 0; i < 4; i++) {
		content[i+8] = 0xff & (value.length() >> (i*8));
	}
    for (char c : value) {
      content.push_back((u8)c);
    }
    checksum = utils::crc16(content);

    fd = open(filename.c_str(), O_WRONLY | O_APPEND);
    offset = lseek(fd, 0, SEEK_END);
    len = content.size() + 3;
    bytes = new u8[len];
    // 构建ventry的头部
    bytes[0] = magic;
    bytes[1] = checksum & 0xff;
    bytes[2] = checksum & 0xff00;
    copy(content.begin(), content.end(), bytes + 3);
    write(fd, bytes, len);
	close(fd);
    delete [] bytes;

    return offset;
}
// 根据偏移量和打开的文件描述符读取一个ventry出来
VEntry
VLog::readvent(u64 offset, int fd) {
    int vlen, ret;
    string value;
    u8 header[15], *bytes;
    u16 checksum;
    u64 key;

    ret = lseek(fd, offset, SEEK_SET);
    read(fd, header, 15);
    
    checksum = loadu(header + 1, 2);
    key = loadu(header + 3, 8);
    vlen = (int)loadu(header + 11, 4);

    bytes = new u8[vlen];
    read(fd, bytes, vlen);
    value = string(bytes, bytes + vlen);
    delete [] bytes;

    VEntry vent(key, vlen, move(value));
    return vent;
}
string
VLog::get(u64 offset) {
    int fd;
    string value;
    u8 header[15], *bytes;
    u16 checksum;
    u64 key;
    int vlen, ret;

    fd = open(filename.c_str(), O_RDONLY);
    ret = lseek(fd, offset, SEEK_SET);
    read(fd, header, 15);
    if (header[0] != 0xff) {
        cout << offset << " " << (int)header[0] << " " << errno << endl;
        cerr << "magic number check fail!" << endl;
        return "";
    }
    checksum = loadu(header + 1, 2);
    key = loadu(header + 3, 8);
    vlen = (int)loadu(header + 11, 4);

    bytes = new u8[vlen];
    read(fd, bytes, vlen);
    value = string(bytes, bytes + vlen);
    delete [] bytes;
    bytes = nullptr;

    // checksum
    
    close(fd);
    return value;
}

/* ******************** KVStore ******************** */
// merge sstables, in persistence, free and realloc memory
int KVStore::merge(list<SSTable *> &stab_lst, int tolevel) {
    
    priority_queue<Sent_pir, vector<Sent_pir>, decltype(&compare_sent_pir)> heap(compare_sent_pir);
    Sent_pir sent_pir;
    vector<SSEntry> tmpsents;
    set<u64> viskey;
    u64 key;
    SSTable *stab;
    SSEntry *sents, *sent;
    u32 cnt;
    string subdir, filename;

    subdir = datadir + "/level-" + to_string(tolevel) + "/";
    /* multi merge */
    for (SSTable * stab : stab_lst) {
        sent = stab->get_next();
        assert(sent != nullptr);
        heap.push(make_pair(sent, stab));
    }
    // merge的时候采用多路归并排序的方法，搞一个堆，堆顶是最小key的元素，然后每次将其加进新sstable
    while(!heap.empty()) {
        sent_pir = heap.top();
        heap.pop();

        key = sent_pir.first->key;
        if (sent_pir.first->vlen > 0 && viskey.find(key) == viskey.end()) {
            tmpsents.push_back(*sent_pir.first);
            viskey.insert(key);
        } else {
            // if repeat, discard the latter key

        }
        sent = sent_pir.second->get_next();
        if (sent) {
            assert(sent != nullptr);
            heap.push(make_pair(sent, sent_pir.second));
        }
    }
    // 合并完的文件删掉。只保留新文件
    for (SSTable * stab : stab_lst) {
        utils::rmfile(stab->filename);
        delete stab;
    }

    stab_lst.clear();
    sort(tmpsents.begin(), tmpsents.end(), compare_ssentry);

    // construct sstables
    for (size_t i = 0, j; i < tmpsents.size(); i += maxSSEntry) {
        
        j = min(tmpsents.size(), i + maxSSEntry);
        filename = subdir + to_string(tmpsents[i].key) + "_" + to_string(tmpsents[j - 1].key) + ".sst";

        stab = new SSTable(filename, tolevel, false);
        stab_lst.push_back(stab);
        stab->setdata(tmpsents, i, j);
    }
    return 0;
}

// call this when memtable is full, this function will store the memtable and compact from the level-0
// all processed object will be deleted and reset to nullptr.
// 跳表满了之后需要将其内容合并至sstable文件，逐层向下合入
int
KVStore::compact() {
    SSTable *stab;
    SSTableHead head;
    list<SSTable *> stabs;
    list<string> sstfiles, tolevelfiles;
    bool more = false;

    stab = new SSTable(datadir + "/level-0/memtable.sst", 0, false);
    stab->convert(memtable);
    stab->save();
    delete memtable;
    memtable = new MemTable();
    
    for (int tolevel = 1; ; tolevel++) {
        getsstfiles(datadir, tolevel - 1, sstfiles);
        // 选出要向下合入的文件
        more = selectolds(sstfiles, tolevel - 1);

        if (!more) {
            break;
        }

        vector<pair<u64, u64>> ranges;
        for (string filename : sstfiles) {
            readSSTheader(filename, head);
            ranges.push_back(pair<u64, u64>(head.minkey, head.maxkey));
        }

        // select sstfiles in tolevel intersected with ranges of `extra`
        // 选出下一层有哪些文件位于上面那些文件的key区间，这是需要合并在一起的对象
        getsstfiles(datadir, tolevel, tolevelfiles);
        for (string filename : tolevelfiles) {
            readSSTheader(filename, head);
            if (in_intervals(ranges, make_pair(head.minkey, head.maxkey))) {
                sstfiles.push_back(filename);
            }
        }

        // merge them
        for (string filename : sstfiles) {
            stabs.push_back(new SSTable(filename, tolevel, true));
        }
        merge(stabs, tolevel);
        
    }
    // free memory
    for (SSTable * stab : stabs) {
        delete stab;
    }
    return 0;
}

int
KVStore::check_compact() {
    if (memtable->full()) {
        compact();
    }
    return 0;
}

KVStore::KVStore(const std::string &dir, const std::string &vlog) : KVStoreAPI(dir, vlog) {
	datadir = dir;
    vlog_name = vlog;
    if (useBloomFilter) {
        maxSSEntry = ( MAX_SSTABLE_SIZE - BLOOM_SIZE ) / sizeof(SSEntry);
    } else {
        maxSSEntry = MAX_SSTABLE_SIZE / sizeof(SSEntry);
    }
    memtable = new MemTable();
    this->vlog = new VLog(vlog_name);

}

KVStore::~KVStore()
{
    delete memtable;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
 // 添加新key-value的时候直接加进跳表即可
void KVStore::put(uint64_t key, const std::string &s)
{
    MemEntry *ment;
    u64 off_v;
    KEY_STATE state;

    state = memtable->search(key, ment);
    if (state == NOT_FOUND) {
        ment = memtable->add(key);
    } else if (state == DEAD) {
        ment->dead = false;
    }
    off_v = vlog->append(key, s);
    ment->sent.offset = off_v;
    ment->sent.vlen = s.length();
    check_compact();
}

// find the key in sstable, return the `SSEntry`, omit the deleted key
SSEntry *
KVStore::find_sstable(u64 key) {
    // find in sstable
    list<string> sstfiles;
    int ret;
    SSTableHead head;
    SSTable *sstable;
    SSEntry *sent;

    for (int level = 0; ;level++) {
        if (!existLevelDir(datadir, level)) {
            break;
        }
        getsstfiles(datadir, level, sstfiles);
        
        for (string sstfile : sstfiles) {
            readSSTheader(sstfile, head);
            if (head.minkey <= key && key <= head.maxkey) {
                sstable = new SSTable(sstfile, level);
                sent = sstable->find(key);
                if (sent->key == key && sent->vlen > 0) {
                    // vlen == 0 means it's deleted
                    return sent;
                }
                delete sstable;
            }
        }
    }
    return nullptr;
}

// find keys in all sstables between [key1, key2], append them in `sents`
void
KVStore::find_sstable_range(u64 key1, u64 key2, vector<SSEntry> &sents, set<u64> &viskey) {
    list<string> sstfiles;
    int ret;
    u32 cnt;
    SSTableHead head;
    SSTable *sstable;
    SSEntry *sent;

    // 遍历所有sstable文件，根据最小最大值判断key是否可能存在，然后再读出来去找
    for (int level = 0; ;level++) {
        if (!existLevelDir(datadir, level)) {
            break;
        }
        getsstfiles(datadir, level, sstfiles);
        
        for (string sstfile : sstfiles) {
            readSSTheader(sstfile, head);
            if (head.maxkey < key1 || head.minkey > key2) {
                continue;
            }

            sstable = new SSTable(sstfile, level);
            sent = (SSEntry *)(sstable->data + sizeof(SSTableHead));
            cnt = head.cnt;
            for (int i = 0; i < cnt; i++) {
                if (key1 <= sent->key && sent->key <= key2 && sent->vlen > 0 &&
                    viskey.find(sent->key) == viskey.end()) {
                    sents.push_back(*sent);
                    viskey.insert(sent->key);
                }
                sent++;
            }
            delete sstable;
        }
    }
}

// get SSEntry instead of value, return 0 when find the key
// 寻找一个key时，先在跳表里找，再在sstable里找
KEY_STATE
KVStore::get_sent(uint64_t key, SSEntry &sent) {
    MemEntry *ment;
    SSEntry *sent_p;
    u64 off_v, offset;
    KEY_STATE state;

    state = memtable->search(key, ment);
    if (state != NOT_FOUND) {
        if (state == LIVE) {
            sent = ment->sent;
            return LIVE;
        } else {
            return DEAD;
        }
    }

    // NOT_FOUND in memtable, so try find in sstable

    sent_p = find_sstable(key);
    if (sent_p) {
        sent = *sent_p;
        return LIVE;
    }
    return NOT_FOUND;
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    SSEntry sent;
    u64 off_v;
    int ret;

    ret = get_sent(key, sent);
    if (ret != LIVE) {
        return "";
    } else {
        return vlog->get(sent.offset);
    }
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    KEY_STATE state;
    int level;
    SSEntry *sent;

    state = memtable->erase(key); // here's a problem, if not exist in sstable, still a delete record will be inserted into memtable
    if (state == LIVE) {
        return true;
    }
    // 如果内存中找到，则根据是否dead返回
    if (state == DEAD) {
        return false;
    }
    // 否则，就再去sstable里找一下
    sent = find_sstable(key);
    if (sent == nullptr) {
        return false;
    }
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    list<string> sstfiles;
    int level, ret;

    for (level = 0; ; level++) {
        if (!existLevelDir(datadir, level)) {
            break;
        }
        getsstfiles(datadir, level, sstfiles);
        
        for (string filename : sstfiles) {
            utils::rmfile(filename);
        }
    }
    delete memtable;
    memtable = new MemTable();
    delete vlog;
    vlog = new VLog(vlog_name);

    gtimestamp = 0;
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
{
    vector<SSEntry> sents;
	MemEntry *ent1, *ent2, *cur;
    set<u64> viskey;
    string value;
    u64 offset;

    // scan memtable
    list.clear();
    ent1 = memtable->search_noless(key1);
    ent2 = memtable->search_noless(key2);
    if (ent2->sent.key == key2) {
        ent2 = ent2->forward[0];
    }
    for (cur = ent1; cur != ent2; cur = cur->forward[0]) {
        if (cur->dead == false && viskey.find(cur->sent.key) == viskey.end()) {
            sents.push_back(cur->sent);
            viskey.insert((cur->sent.key));
        }
        // value = vlog->get(cur->sent.offset);
        // list.push_back(pair<u64, string>(cur->sent.key, value));
    }

    // scan sstables
    find_sstable_range(key1, key2, sents, viskey);


    sort(sents.begin(), sents.end(), compare_ssentry);
    for (SSEntry sent : sents) {
        value = vlog->get(sent.offset);
        list.push_back(pair<u64, string>(sent.key, value));
    }

    return;
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size)
{
    list<pair<u64, VEntry>> vents;
    off_t start_off, offset;
    int fd, ret;
    u64 key;
    SSEntry sent;

    // fetch some data into mem and free the corresponding disk space
    start_off = utils::seek_data_block(vlog_name);
    offset = start_off;
    fd = open(vlog_name.c_str(), O_RDONLY);

    while (offset - start_off < chunk_size) {
        vents.push_back(make_pair(offset, vlog->readvent(offset, fd)));
        offset += vents.back().second.size();
    }

    close(fd);
    utils::de_alloc_file(vlog_name, start_off, offset - start_off);

    // re-insert fresh vlog
    // 将取出的字符串值重新插入
    for (auto pir : vents) {
        key = pir.second.key;
        ret = get_sent(key, sent);
        // 如果发现该key现在查出的值和当前ventry不是同一个，那当前这个ventry可以丢弃，不插入
        if (ret == LIVE && sent.offset != pir.first) {
            // find but point to another vlog entry, discard it
            continue;
        }
        // 重新将ventry插入
        put(key, pir.second.value);
    }
    compact();
}