package com.github.lisama569.localmessagequeuejava.mq.store;

import com.github.lisama569.localmessagequeuejava.mq.model.TopicPartition;

/**
 * 队列层的每分区已确认位点（排他）持久化：确认发生后即落盘，
 * 使重启后无需消费者接管即可按位点重建未确认计数、复用容量。
 */
public interface AckWatermarkStore {

    /** 该分区已确认到的位点（排他）；无记录返回 0。 */
    long load(TopicPartition tp);

    /** 持久化该分区的已确认位点（排他），调用方保证按分区单调推进。 */
    void store(TopicPartition tp, long committedOffset);
}
