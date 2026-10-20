#include <metal_stdlib>
using namespace metal;

// repr(C) Slot on the CPU: byte key[20], uint32 index, uint32 occupancy.
struct Slot {
    uchar key[20];
    uint guide_index;
    uint occupied;
};
static_assert(sizeof(Slot) == 28, "Slot must have a 28-byte stride");

kernel void guide_lookup(device const Slot *table [[buffer(0)]],
                         device const uchar *reads [[buffer(1)]],
                         device uint *indices [[buffer(2)]],
                         constant uint &slot_count [[buffer(3)]],
                         constant uint &read_count [[buffer(4)]],
                         uint tid [[thread_position_in_grid]]) {
    if (tid >= read_count) return;
    device const uchar *key = reads + tid * 20;
    uint hash = 2166136261u;
    for (uint j = 0; j < 20; ++j) {
        hash = (hash ^ uint(key[j])) * 16777619u;
    }
    const uint mask = slot_count - 1;
    uint slot = hash & mask;
    uint match = 0xffffffffu;
    // Bounded even if a corrupted table is full. Valid tables are <=50% full.
    for (uint probe = 0; probe < slot_count; ++probe) {
        if (table[slot].occupied == 0) break;
        bool equal = true;
        for (uint j = 0; j < 20; ++j) {
            if (key[j] != table[slot].key[j]) {
                equal = false;
                break;
            }
        }
        if (equal) {
            match = table[slot].guide_index;
            break;
        }
        slot = (slot + 1) & mask;
    }
    indices[tid] = match;
}
