#include "RdxMergeOperator.h"

RdxMergeOperator::RdxMergeOperator() : merger() {}

bool RdxMergeOperator::FullMergeV2(const MergeOperationInput& merge_in,
                                   MergeOperationOutput* merge_out) const {
    merge_out->new_value.clear();
    std::vector<rocksdb::Slice> operands(merge_in.operand_list);
    if (merge_in.existing_value){
        operands.push_back(*merge_in.existing_value);
    }
    try {
        merge_out->new_value = merger.merge(operands);
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Merge error: " << e.what() << std::endl;
        return false;
    }
}

const char* RdxMergeOperator::Name() const {
    return "RdxMergeOperator";
}
