// 解析Maxwell抽取过来的binlog，获取到新值data和旧值old
GenericRowData after = (GenericRowData) row.getRow(0, fieldCount); 
GenericRowData before = (GenericRowData) row.getRow(1, fieldCount); 
// 对新、旧值赋予Flink Rowkind枚举
before.setRowKind(RowKind.UPDATE_BEFORE);
after.setRowKind(RowKind.UPDATE_AFTER);
// 对新、旧值赋予新值的业务意义的更新时间
if (this.extendFieldIndex != null) {
    after.setField(this.extendFieldIndex.f0, after.getField(this.extendFieldIndex.f1));
    before.setField(this.extendFieldIndex.f0, after.getField(this.extendFieldIndex.f1));
}
// 将1条update记录拆分为2条记录输出
emitRow(row, before, out);
emitRow(row, after, out);
