private boolean getClusterStatus(ProxyBackendConfiguration backend, int retryNum){
    if(retryNum == 3){
        return false;
    }
    // 获取集群地址
    String target = backend.getProxyTo();
    // 从缓存中获取登录token
    String clusterToken = getClusterToken(String.format("%s_token", backend.getName()));
    // 如果缓存中没有，或者缓存过期
    if (Strings.isNullOrEmpty(clusterToken)){
        String reLoginToken = HttpRequestUtils.getTrinoClusterToken(target, monitorConfiguration);
        log.info("First login and get token or cache expired with {}", backend.getName());
        setClusterTokenCache(String.format("%s_token", backend.getName()), reLoginToken);
        return getClusterStatus(backend, retryNum + 1);
    }
    // 获取返回体
    String response = HttpRequestUtils.requestTrinoClusterStatsWithToken(target, clusterToken);
    // 如果token无效，集群无响应，循环301跳转，或者其他原因导致的空返回体
    if (Strings.isNullOrEmpty(response)) {
        log.error("Received null/empty response for {}", target);
        String reLoginToken = HttpRequestUtils.getTrinoClusterToken(target, monitorConfiguration);
        setClusterTokenCache(String.format("%s_token", backend.getName()), reLoginToken);
    
    }
    try {
        HashMap<String, Object> result = null;
        result = OBJECT_MAPPER.readValue(response, HashMap.class);
        int activeWorkers = (int) result.get("activeWorkers");
        // 返回体正常，并且activeWorkers至少为1
        if (activeWorkers > 0){
            return true;
        }
        // 其他的判别逻辑...
        } catch (Exception e) {
        log.error("Error parsing cluster stats from [{}]", response, e);
    }
    return false;
}
