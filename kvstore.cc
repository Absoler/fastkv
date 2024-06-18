#include "kvstore.h"
#include "utils.h"
#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <string>
#include <sys/types.h>
#include <unistd.h>
using namespace std;

/* ******************** my utils ******************** */
bool useBloomFilter = false;
bool useCache = true;
size_t maxSSEntry;
u64 gtimestamp; // sequence number of SSTable
string datadir;

bool in_intervals(const vector<pair<u64, u64>> &intervals, pair<u64, u64> range) {
    for (auto pir : intervals) {
        if (pir.first <= range.first && range.first <= pir.second || pir.first <= range.second && range.second <= pir.second) {
            return true;
        }
    }
    return false;
}

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
        return p1.second->head.timestamp < p2.second->head.timestamp;
}
// read a sstable header from given filename
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

    state = search(key, ent);
    if (state == NOT_FOUND) {
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
    head.minkey = min(key, head.minkey);
    head.maxkey = max(key, head.maxkey);
}

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


    if (needloadsents) {
        readSSTheader(filename, head);
        loadsents();
        curp = get_content();
    } else {
        head.cnt = 0;
        head.minkey = -1ULL;
        head.maxkey = 0;
        head.timestamp = gtimestamp;
        gtimestamp++;
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

    head = (SSTableHead){head.timestamp, end - start, sent_vec[start].key,
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
        cerr << "can't create VLog file with errno " << errno << endl;
        return false;
    }
    close(fd);
    return true;
}

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
    bytes[0] = magic;
    bytes[1] = checksum & 0xff;
    bytes[2] = checksum & 0xff00;
    copy(content.begin(), content.end(), bytes + 3);
    write(fd, bytes, len);
	close(fd);
    delete [] bytes;

    return offset;
}

off_t
VLog::get_start_off() {
    off_t base, off;
    int fd;
    u8 tmp[1];

    base = utils::seek_data_block(filename);
    fd = open(filename.c_str(), O_RDWR);
    lseek(fd, base, SEEK_SET);
    for (off = base; off < base + BLOCK_SIZE; off++) {
        read(fd, tmp, 1);
        if (tmp[0] == 0xff) {
            break;
        }
    }
    return off;
}

int
VLog::readvent(u64 offset, int fd, VEntry &vent) {
    int vlen, ret;
    string value;
    u8 header[15], *bytes;
    u16 checksum;
    u64 key;
    vector<u8> content(12, 0);

    lseek(fd, offset, SEEK_SET);
    ret = read(fd, header, 15);
    if (ret <= 0) {
        cerr << "read vlog entry EOF or err with: " << ret << endl;
        return -1;
    }
    checksum = loadu(header + 1, 2);
    key = loadu(header + 3, 8);
    vlen = (int)loadu(header + 11, 4);

    bytes = new u8[vlen];
    read(fd, bytes, vlen);
    value = string(bytes, bytes + vlen);

    for (int i = 0; i < 8; i++) {
		content[i] = 0xff & (key >> (i*8));
	}
    for (int i = 0; i < 4; i++) {
		content[i+8] = 0xff & (value.length() >> (i*8));
	}
    for (char c : value) {
        content.push_back((u8)c);
    }
    delete [] bytes;

    vent = VEntry(key, vlen, move(value));
    return 0;
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
        // cout << offset << " " << (int)header[0] << " " << errno << endl;
        // cerr << "magic number check fail!" << endl;
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
        // assert(sent != nullptr);
        if (sent)
        heap.push(make_pair(sent, stab));
    }
    while(!heap.empty()) {
        sent_pir = heap.top();
        heap.pop();

        key = sent_pir.first->key;
        if (sent_pir.first->vlen > 0 && viskey.find(key) == viskey.end()) {
            tmpsents.push_back(*sent_pir.first);
        } else {
            // if repeat, discard the latter key

        }
        viskey.insert(key);
        sent = sent_pir.second->get_next();
        if (sent) {
            heap.push(make_pair(sent, sent_pir.second));
        }
    }
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
        getsstfiles(datadir, tolevel, tolevelfiles);
        for (string filename : tolevelfiles) {
            readSSTheader(filename, head);
            if (in_intervals(ranges, make_pair(head.minkey, head.maxkey))) {
                sstfiles.push_back(filename);
            }
        }

        // merge them
        for (string filename : sstfiles) {
            stabs.push_back(new SSTable(filename, tolevel));
        }
        merge(stabs, tolevel);
        
        for (SSTable * stab : stabs) {
            delete stab;
        }
    stabs.clear();
    }
    // free memory
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
    if (ret != LIVE || sent.vlen == 0) {
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
    if (state == DEAD) {
        return false;
    }
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
    VEntry vent;
    off_t start_off, offset;
    int fd, ret;
    u64 key;
    SSEntry sent;

    // fetch some data into mem and free the corresponding disk space
    start_off = vlog->get_start_off();
    offset = start_off;
    fd = open(vlog_name.c_str(), O_RDONLY);

    while (offset - start_off < chunk_size) {
        ret = vlog->readvent(offset, fd, vent);
        if (ret != 0) {
            break;
        }
        vents.push_back(make_pair(offset, vent));
        offset += vents.back().second.size();
    }

    close(fd);
    utils::de_alloc_file(vlog_name, start_off, offset - start_off);

    // re-insert fresh vlog
    for (auto pir : vents) {
        key = pir.second.key;
        ret = get_sent(key, sent);
        if (ret != LIVE || ret == LIVE && sent.offset != pir.first || sent.vlen == 0) {
            // find but point to another vlog entry, discard it
            continue;
        }
        put(key, pir.second.value);
    }
    compact();
}