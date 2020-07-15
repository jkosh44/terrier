#include "storage/sql_table.h"

#include <map>
#include <set>
#include <string>
#include <vector>

#include "common/macros.h"
#include "storage/storage_util.h"

namespace terrier::storage {

SqlTable::SqlTable(const common::ManagedPointer<BlockStore> store, const catalog::Schema &schema)
    : block_store_(store) {
  // Begin with the NUM_RESERVED_COLUMNS in the attr_sizes
  std::vector<uint16_t> attr_sizes;
  attr_sizes.reserve(NUM_RESERVED_COLUMNS + schema.GetColumns().size());

  for (uint8_t i = 0; i < NUM_RESERVED_COLUMNS; i++) {
    attr_sizes.emplace_back(8);
  }

  TERRIER_ASSERT(attr_sizes.size() == NUM_RESERVED_COLUMNS,
                 "attr_sizes should be initialized with NUM_RESERVED_COLUMNS elements.");

  for (const auto &column : schema.GetColumns()) {
    attr_sizes.push_back(column.AttrSize());
  }

  auto offsets = storage::StorageUtil::ComputeBaseAttributeOffsets(attr_sizes, NUM_RESERVED_COLUMNS);

  ColumnMap col_oid_to_id;
  // Build the map from Schema columns to underlying columns
  for (const auto &column : schema.GetColumns()) {
    switch (column.AttrSize()) {
      case VARLEN_COLUMN:
        col_oid_to_id[column.Oid()] = col_id_t(offsets[0]++);
        break;
      case 8:
        col_oid_to_id[column.Oid()] = col_id_t(offsets[1]++);
        break;
      case 4:
        col_oid_to_id[column.Oid()] = col_id_t(offsets[2]++);
        break;
      case 2:
        col_oid_to_id[column.Oid()] = col_id_t(offsets[3]++);
        break;
      case 1:
        col_oid_to_id[column.Oid()] = col_id_t(offsets[4]++);
        break;
      default:
        throw std::runtime_error("unexpected switch case value");
    }
  }

  auto layout = storage::BlockLayout(attr_sizes);
  table_ = {new DataTable(block_store_, layout, layout_version_t(0)), layout, col_oid_to_id};
}

std::vector<col_id_t> SqlTable::ColIdsForOids(const std::vector<catalog::col_oid_t> &col_oids) const {
  TERRIER_ASSERT(!col_oids.empty(), "Should be used to access at least one column.");
  std::vector<col_id_t> col_ids;

  // Build the input to the initializer constructor
  for (const catalog::col_oid_t col_oid : col_oids) {
    TERRIER_ASSERT(table_.column_map_.count(col_oid) > 0, "Provided col_oid does not exist in the table.");
    const col_id_t col_id = table_.column_map_.at(col_oid);
    col_ids.push_back(col_id);
  }

  return col_ids;
}

ProjectionMap SqlTable::ProjectionMapForOids(const std::vector<catalog::col_oid_t> &col_oids) {
  // Resolve OIDs to storage IDs
  auto col_ids = ColIdsForOids(col_oids);

  // Use std::map to effectively sort OIDs by their corresponding ID
  std::map<col_id_t, catalog::col_oid_t> inverse_map;
  for (uint16_t i = 0; i < col_oids.size(); i++) inverse_map[col_ids[i]] = col_oids[i];

  // Populate the projection map using the in-order iterator on std::map
  ProjectionMap projection_map;
  uint16_t i = 0;
  for (auto &iter : inverse_map) projection_map[iter.second] = i++;

  return projection_map;
}

void SqlTable::Reset() { table_.data_table_->Reset(); }

void SqlTable::CopyTable(const common::ManagedPointer<transaction::TransactionContext> txn,
                         const common::ManagedPointer<DataTable> src){
  uint32_t filled = 0;
  auto it = src->begin();
  std::vector<catalog::col_oid_t> col_oids;
  for(auto &cols : table_.column_map_){
    col_oids.push_back(cols.first);
  }
  auto pr_init = InitializerForProjectedRow(col_oids);
  void *buffer = alloca(pr_init.ProjectedRowSize());
  auto *projected_row = pr_init.InitializeRow(buffer);
  while (it != end()) {
    const TupleSlot slot = *it;
    // Only fill the buffer with valid, visible tuples
    if(!Select(txn, slot, projected_row)){
      it++;
      continue;
    }

    //TODO(tanujnay112) I don't like how I have to hardcode this
    auto *redo = txn->StageWrite(catalog::db_oid_t(999), catalog::table_oid_t (999), pr_init);
    auto *new_pr = redo->Delta();
    auto pr_map = ProjectionMapForOids(col_oids);
    for(auto &cols : table_.column_map_){
      auto offset = pr_map[cols.first];
      auto new_pr_ptr = new_pr->AccessForceNotNull(offset);
      auto src_ptr = new_pr->AccessWithNullCheck(offset);
      if(src_ptr == nullptr){
        new_pr->SetNull(offset);
        continue;
      }
      std::memcpy(new_pr_ptr, src_ptr, table_.layout_.AttrSize(cols.second));

      // copy over varlens contents
      if(table_.layout_.IsVarlen(cols.second)){
        auto varlen = reinterpret_cast<storage::VarlenEntry*>(src_ptr);
        if(varlen->NeedReclaim()){
          byte *new_allocation = new byte[varlen->Size()];
          std::memcpy(new_allocation, varlen->Content(), varlen->Size());
          auto new_varlen = VarlenEntry::Create(new_allocation, varlen->Size(), true);
          *reinterpret_cast<storage::VarlenEntry*>(new_pr_ptr) = new_varlen;
        }
      }
    }
    Insert(txn, redo);
    it++;
  }
}

catalog::col_oid_t SqlTable::OidForColId(const col_id_t col_id) const {
  const auto oid_to_id = std::find_if(table_.column_map_.cbegin(), table_.column_map_.cend(),
                                      [&](const auto &oid_to_id) -> bool { return oid_to_id.second == col_id; });
  return oid_to_id->first;
}

}  // namespace terrier::storage
