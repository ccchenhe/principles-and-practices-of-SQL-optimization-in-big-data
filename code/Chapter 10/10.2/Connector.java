/** 从定义的schema信息中，获取到每一个列族里，作为版本的时间戳字段的索引/位置，返回例如{"c": 1, "cf": 3}，代表cf列族中，索引为3的元素作为版本时间戳
 */
public static Map<String, Integer> getCombineColumnRowDataIndex(HBaseTableSchema schema, Map<String, String> mergeMultipleCombineColumnMap){
    Map<String, Integer> result = new HashMap<>(2);
    if(mergeMultipleCombineColumnMap.isEmpty()){
        return result;
    }

    for(String i: mergeMultipleCombineColumnMap.keySet()){
        for(String q: schema.getFamilyNames()){
            if(q.equals(i)){
                Map<String, Integer> qualifierNameMap = schema.getQualifierMap(q);
                String targetColumnAppointFamilyKey = mergeMultipleCombineColumnMap.get(q);
                result.put(q, qualifierNameMap.get(targetColumnAppointFamilyKey));
            }
        }
    }
    return result;
}



public Put createPutMutation(Row row, Map < String, Integer > combineColumnNameConfigMap) {
        Put put = new Put(rowkey);
        for (int i = 0; i < fieldLength; i++) {
            int f = i > rowKeyIndex ? i - 1 : i;
            // 获取rowkey
            byte[] familyKey = families[f];
            Row familyRow = (Row) row.getField(i);
            for (int q = 0; q < this.qualifiers[f].length; q++) {
            // 获取列
            byte[] qualifier = qualifiers[f][q];
            int typeIdx = qualifierTypes[f][q];
            // 获取值
            byte[] value = HBaseTypeUtils.serializeFromObject(familyRow.getField(q), typeIdx, charset);
            // 如果传递了指定时间戳的参数，在方法中获取到对应字段的索引位置，并取出值，作为put方法的传递
            if (CombineColumnUtils.judgeHasValue(combineColumnNameConfigMap, Bytes.(familyKey))) {
                // 获取对应字段的索引位置
                Integer combineColumnIndex = combineColumnNameConfigMap.get(Bytes.(familyKey));
                byte[] combineValue = HBaseTypeUtils.serializeFromObject(familyRow.getField(combineColumnIndex), qualifierTypes[f][combineColumnIndex], charset);
                // 获取对应索引的字段的值
                String fillZeroCombineValue = CombineColumnUtils.fillInputTimestamp(HBaseTypeUtils.deserializeToObject(combineValue, qualifierTypes[f][combineColumnIndex], charset).());
                // 调用带有timestamp的put方法
                put.addColumn(familyKey, qualifier, Long.parseLong(fillZeroCombineValue), value);
            } else {
                // 调用普通的put方法
                put.addColumn(familyKey, qualifier, value);
            }
            }
        }
        return put;
}
