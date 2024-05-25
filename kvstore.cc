#include "kvstore.h"
#include "utils.h"
#include <iostream>
using namespace std;

/* ******************** my utils ******************** */
u64 loadint(u8 *bytes, int n) {
    // little endian load
    u64 val = 0;
    for (int i = 0; i < n; i++) {
        val = (val << 8) + bytes[n - i - 1];
    }
    return val;
}
/* ******************** my utils ******************** */

Skiplist::Skiplist() {
    head = new SSEntry(-1, -1, -1);
    maxLevel = 1;
    dis = std::uniform_real_distribution<double>(0.0, 1.0);
}


// the next of the closet less element
SSEntry *
Skiplist::search(u64 key) {
    SSEntry *cur = this->head;

    for (int i = maxLevel; i >= 0; i--) {
        if (!cur->forward[i])
            continue;
        while (cur->forward[i] && cur->forward[i]->key < key) {
            cur = cur->forward[i];
        }
    }
    return cur->forward[0];
}
SSEntry *
Skiplist::search_equ(u64 key) {
    SSEntry *cur = search(key);
    
    if (cur && cur->key == key) {
        return cur;
    }
    return nullptr;
}

SSEntry *
Skiplist::add(u64 key) {
    std::vector<SSEntry *> updates(MAX_SKIPLIST_LEVEL, head);
    SSEntry *cur = this->head;
    for (int i = maxLevel - 1; i >= 0; i--) {
        while (cur->forward[i] && cur->forward[i]->key < key) {
        cur = cur->forward[i];
        }
        updates[i] = cur;
    }
    // level of the new node
    int lv = randomLv();
    maxLevel = std::max(lv, maxLevel);
    // the new node
    SSEntry *newnode = new SSEntry(key, -1, -1, lv);
    for (int i = 0; i < lv; i++) {
        
        newnode->forward[i] = updates[i]->forward[i];
        updates[i]->forward[i] = newnode;
    }
    return newnode;
}

int Skiplist::randomLv() {
    int lv = 1;
    while(dis(gen) <= SKIPLIST_P && lv < MAX_SKIPLIST_LEVEL){
        lv ++;
    }
    return lv;
}

bool Skiplist::erase(u64 key) {
    std::vector<SSEntry *> updates(MAX_SKIPLIST_LEVEL, nullptr);
    SSEntry *cur = head;

    for (int i = maxLevel - 1; i >= 0; i--) {
        while (cur->forward[i] && cur->forward[i]->key < key) {
        cur = cur->forward[i];
        }
        updates[i] = cur;
    }

    cur = cur->forward[0];
    if (!cur || cur->key != key) {
        return false;
    }
    for (int i = 0; i < maxLevel; i++) {
        if (updates[i]->forward[i] != cur) {
        break;
        // non-0 level index may overlap the `num` node
        }
        updates[i]->forward[i] = cur->forward[i];
    }
    delete cur;

    while (maxLevel > 1 && head->forward[maxLevel - 1] == nullptr) {
        maxLevel--;
    }
    return true;
}


SSTable::SSTable(const string &dir, const string &vlog_name) {
    memtable = new Skiplist();
    vlog = new VLog(vlog_name);
}

void
SSTable::put(u64 key, const std::string &value) {
    SSEntry *ent_s;
    u64 off_v;
    

    ent_s = memtable->search_equ(key);
    if (ent_s == nullptr) {
        ent_s = memtable->add(key);
    }
    off_v = vlog->append(key, value);
    ent_s->offset = off_v;
    ent_s->vlen = value.length();
}

string 
SSTable::get(u64 key) {
    SSEntry *ent_s;
    u64 off_v;

    ent_s = memtable->search_equ(key);
    if (ent_s == nullptr) {
        // cout << "there's no key " << key << endl;
        return "";
    }
    off_v = ent_s->offset;
    return vlog->get(off_v);
}


void
SSTable::scan(u64 key1, u64 key2, std::list<std::pair<u64, std::string> > &list) {
    SSEntry *ent1, *ent2, *cur;
    string value;
    u64 offset;

    list.clear();
    ent1 = memtable->search(key1);
    ent2 = memtable->search(key2);
    if (ent2->key == key2) {
        ent2 = ent2->forward[0];
    }
    for (cur = ent1; cur != ent2; cur = cur->forward[0]) {
        value = vlog->get(cur->offset);
        list.push_back(pair<u64, string>(cur->key, value));
    }

    return;
}

bool
SSTable::del(u64 key) {
    return memtable->erase(key);
}


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
    checksum = loadint(header + 1, 2);
    key = loadint(header + 3, 8);
    vlen = (int)loadint(header + 11, 4);

    bytes = new u8[vlen];
    read(fd, bytes, vlen);
    value = string(bytes, bytes + vlen);
    delete [] bytes;

    // checksum
    
    return value;
}

KVStore::KVStore(const std::string &dir, const std::string &vlog) : KVStoreAPI(dir, vlog)
{
	sstable = new SSTable(dir, vlog);
}

KVStore::~KVStore()
{
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	sstable->put(key, s);
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	return sstable->get(key);
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	return sstable->del(key);
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
	sstable->scan(key1, key2, list);
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size)
{
}