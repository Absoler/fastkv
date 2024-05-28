#include "kvstore.h"
#include "utils.h"
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <ios>
#include <iostream>
#include <unistd.h>
using namespace std;

/* ******************** my utils ******************** */
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

// get sstable filenames from given level
int getsstfiles(const string &dir, int level, vector<string> &sstfiles) {
    vector<string> files;
    string subdir = dir + "/" + to_string(level) + "/";

    sstfiles.clear();
    if (!utils::dirExists(subdir)) {
        return -1;
    }
    utils::scanDir(subdir, files);
    for (string file : files) {
        size_t len = file.length();
        if (len >= 4 && file.substr(len-4).compare(".sst") == 0) {
            sstfiles.push_back(subdir + file);
        }
    }
    return 0;
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
MemTable::search_less(u64 key) {
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
MemEntry *
MemTable::search(u64 key) {
    MemEntry *cur = search_less(key);
    
    if (cur && cur->sent.key == key) {
        return cur;
    }
    return nullptr;
}

// return when the key is live
MemEntry *
MemTable::search_live(u64 key) {
    MemEntry *cur = search(key);
    if (cur && cur->dead == false) {
        return cur;
    }
    return nullptr;
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
bool MemTable::erase(u64 key) {
    MemEntry *ent;
    bool found = false;

    ent = search_live(key);
    if (ent) {
        found = true;
    } else {
        // not find live `key`
        ent = search(key);
        if (!ent) {
            ent = add(key);
        }
    }
    ent->dead = true;
    return found;
}

/* ******************** SSTable ******************** */
void
SSTable::updateKey(u64 key) {
    head.minkey = min(key, head.minkey);
    head.maxkey = max(key, head.maxkey);
}

void
SSTable::loaddata() {
    int fd;

    fd = open(filename.c_str(), O_RDONLY);
    lseek(fd, sizeof(SSTableHead), SEEK_SET);
    read(fd, data, head.cnt * sizeof(SSEntry));
    close(fd);
}

SSTable::SSTable(const string &filename) {
    this->filename = filename;
    data = nullptr;

    head.cnt = 0;
    head.minkey = -1ULL;
    head.maxkey = 0;
    if (useBloomFilter) {
        maxEntries = ( MAX_SSTABLE_SIZE - BLOOM_SIZE ) / sizeof(SSEntry);
    } else {
        maxEntries = MAX_SSTABLE_SIZE / sizeof(SSEntry);
    }

    readSSTheader(filename, head);
    loaddata();
}

SSTable::~SSTable() {
    delete data;
}

void
SSTable::put(u64 key, const std::string &value) {

}


void
SSTable::scan(u64 key1, u64 key2, std::list<std::pair<u64, std::string> > &list) {
    return;
}

// may find the wrong entry (key not equal), need recheck
SSEntry *
SSTable::find(u64 key) {
    if (!(head.minkey <= key && key <= head.maxkey)) {
        cerr << "try to get a key not in range" << endl;
        return (SSEntry *)(data + sizeof(SSTableHead));
    }
    int lo, hi, mid;
    SSEntry *sents;

    // binary search in sstable
    lo = 0, hi = head.cnt;
    sents = (SSEntry *)(data + sizeof(SSTableHead));
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
    data = new u8[sizeof(SSTableHead) + memtable->size * sizeof(SSEntry)];
    // write header
    memcpy(data, &head, sizeof(SSTableHead));

    // write bloom filter

    // write data
    ment = memtable->head->forward[0];
    while (ment) {
        memcpy(data + sizeof(SSTableHead) + cnt * sizeof(SSEntry), &ment->sent, sizeof(SSEntry));
        ment = ment->forward[0];
        cnt += 1;
    }
}

/* ******************** VLog ******************** */
bool
VLog::createfile() {
    int fd;

    fd = open(filename.c_str(), O_WRONLY | O_CREAT);
    if (fd == -1) {
        cerr << "can't create VLog file" << endl;
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

string
VLog::get(u64 offset) {
    int fd;
    string value;
    u8 header[15], *bytes;
    u16 checksum;
    u64 key;
    int vlen;

    fd = open(filename.c_str(), O_RDONLY);
    lseek(fd, offset, SEEK_SET);
    read(fd, header, 15);
    if (header[0] != 0xff) {
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
    
    return value;
}

/* ******************** KVStore ******************** */
// gather current max level, timestamp from sstable file(s)
// void
// KVStore::gatherinfo() {
//     vector<string> subdirs, ssfiles;
//     string dir;
//     vector<int> levels;
//     int minlevel, maxlevel;
//     SSTableHead head;

//     utils::scanDir(curdir, subdirs);
    
//     for (string subdir : subdirs) {
//         if (subdir.substr(0, 5).compare("level") == 0) {
//             levels.push_back(stoi(subdir.substr(5)));
//         }
//     }
//     sort(levels.begin(), levels.end());
//     minlevel = levels[0];
//     maxlevel = *levels.rbegin();
//     dir = curdir + "/level" + to_string(minlevel) + "/";
//     utils::scanDir(dir, ssfiles);
//     for (string filename : ssfiles) {
//         size_t len = filename.length();
//         if (len >= 4 && filename.substr(len - 4).compare(".sst") == 0) {
//             readSSTheader(dir + filename, head);
//             timestamp = max(head.timestamp, timestamp);
//         }
//     }
// }

KVStore::KVStore(const std::string &dir, const std::string &vlog) : KVStoreAPI(dir, vlog), curdir(dir)
{
	curdir = dir;
    vlog_name = vlog;
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

    ment = memtable->search(key);
    if (ment == nullptr) {
        ment = memtable->add(key);
    } else if (ment->dead) {
        ment->dead = false;
    }
    off_v = vlog->append(key, s);
    ment->sent.offset = off_v;
    ment->sent.vlen = s.length();
}

// find the key in sstable, return the `SSEntry`
SSEntry *
KVStore::find_sstable(u64 key) {
    // find in sstable
    vector<string> sstfiles;
    int ret;
    SSTableHead head;
    SSTable *sstable;
    SSEntry *sent;

    for (int level = 0; ;level++) {
        ret = getsstfiles(curdir, level, sstfiles);
        if (ret != 0) {
            break;
        }
        for (string sstfile : sstfiles) {
            readSSTheader(sstfile, head);
            if (head.minkey <= key && key <= head.maxkey) {
                sstable = new SSTable(sstfile);
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

void
KVStore::find_sstable_range(u64 key1, u64 key2, vector<SSEntry> &sents, set<u64> &viskey) {
    vector<string> sstfiles;
    int ret;
    u32 cnt;
    SSTableHead head;
    SSTable *sstable;
    SSEntry *sent;

    for (int level = 0; ;level++) {
        ret = getsstfiles(curdir, level, sstfiles);
        if (ret != 0) {
            break;
        }
        for (string sstfile : sstfiles) {
            readSSTheader(sstfile, head);
            if (head.maxkey < key2 || head.minkey > key1) {
                continue;
            }

            sstable = new SSTable(sstfile);
            sent = (SSEntry *)(sstable->data + sizeof(SSTableHead));
            cnt = head.cnt;
            for (int i = 0; i < cnt; i++) {
              if (key1 <= sent->key && sent->key <= key2 && sent->vlen > 0 &&
                  viskey.find(sent->key) == viskey.end()) {
                sents.push_back(*sent);
                viskey.insert(sent->key);
              }
            }
            delete sstable;
        }
    }
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    MemEntry *ment;
    u64 off_v;

    ment = memtable->search_live(key);
    if (ment) {
        off_v = ment->sent.offset;
        return vlog->get(off_v);
    }

    // find in sstable
    u64 offset;
    SSEntry *sent;

    sent = find_sstable(key);
    if (sent) {
        offset = sent->offset;
        delete sent;
        return vlog->get(offset);
    }
    return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    bool found;
    int level;
    SSEntry *sent;

    found = memtable->erase(key); // here's a problem, if not exist in sstable, still a delete record will be inserted into memtable
    if (found) {
        return found;
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
    ent1 = memtable->search_less(key1);
    ent2 = memtable->search_less(key2);
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
}