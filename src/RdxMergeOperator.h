#ifndef RDXMERGEOPERATOR_H
#define RDXMERGEOPERATOR_H

#include <iostream>
#include "rocksdb/merge_operator.h"
#include "RdxMerger.h"

class RdxMergeOperator : public rocksdb::MergeOperator {
public:
    mutable RdxMerger merger;

    RdxMergeOperator();
    bool FullMergeV2(const MergeOperationInput& merge_in,
                     MergeOperationOutput* merge_out) const override;
    const char* Name() const override;
};

#endif // RDXMERGEOPERATOR_H
