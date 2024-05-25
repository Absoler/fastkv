#pragma once

#include "kvstore_api.h"

#include <string>
#include <vector>
#include <list>
#include <random>

#define MAX_SKIPLIST_LEVEL 32
#define SKIPLIST_P 0.5
#define u64 unsigned long int
#define u32 unsigned int
#define u16 unsigned short
#define u8 unsigned char

/* ******************** my utils ******************** */
u64 loadint(u8 *bytes, int n);
/* ******************** my utils ******************** */

class SSEntry {
public:
  u64 key;
  u64 offset;
  int vlen;
  std::vector<SSEntry *> forward;
  SSEntry(u64 _key, u64 _offset, int _vlen, int maxLevel = MAX_SKIPLIST_LEVEL)
      : key(_key), offset(_offset), vlen(_vlen), forward(maxLevel, nullptr) {}
};


class Skiplist {
public:
    std::mt19937 gen{std::random_device{}()};
    std::uniform_real_distribution<double> dis;
    SSEntry *head;
    int maxLevel;

    Skiplist();
    SSEntry *search(u64 key);
    SSEntry *search_equ (u64 key);
    SSEntry *add(u64 key);
    int randomLv();
    bool erase(u64 key);
};

class VLog;
class SSTable {
    public:
    u64 header[4];
    unsigned char bloom[1<<13];

    Skiplist *memtable;
    VLog *vlog;

    SSTable(const std::string &dir, const std::string &vlog_name);

    void put(u64 key, const std::string &s);
    std::string get(u64 key);
    bool del(u64 key);
    void scan(u64 key1, u64 key2, std::list<std::pair<u64, std::string>> &list);
};

class VEntry {
    public:
    u8  magic;
    u16 checksum;
    u64 key;
    int vlen;
    std::string value;

    VEntry(u64 _key, int _vlen, const std::string &s)
        : key(_key), vlen(_vlen), value(s){}
};
class VLog {
    public:
    friend class SSTable;
    const static u8 magic = 0xff;
    std::string filename;

    VLog(const std::string &name): filename(name) {}

    bool createfile();
    u64 append(u64 key, const std::string &value); // return the offset
    std::string get(u64 offset);    // get the value

};

class KVStore : public KVStoreAPI
{
	// You can add your implementation here
private:
public:
	SSTable *sstable;
	KVStore(const std::string &dir, const std::string &vlog);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

	void gc(uint64_t chunk_size) override;
};
