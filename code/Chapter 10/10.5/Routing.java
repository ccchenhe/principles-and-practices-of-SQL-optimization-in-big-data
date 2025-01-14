/**
 * @param backends: 注册的集群列表
 * @param unHealthyClustersWithScheduler: 缓存中维护的不健康的集群列表
 */
public String generateRoutingCluster(List<ProxyBackendConfiguration> backends, List<String> unHealthyClustersWithScheduler){
    // 如果没有注册任何集群，直接抛异常
    if (backends.size() == 0) {
        throw new IllegalStateException("Number of active backends found zero");
    }

    // 如果没有不健康的集群，直接选择所有经注册的集群列表
    if(unHealthyClustersWithScheduler == null || unHealthyClustersWithScheduler.size() == 0){
        return routingWithClusterLoad(backends);
    }

    // 如果所有集群都是不健康的，还是直接选择所有经注册的集群列表
    if(backends.size() == unHealthyClustersWithScheduler.size()){
        return routingWithClusterLoad(backends);
    }

    // 不健康集群和所有注册集群的差集
    List<ProxyBackendConfiguration> backendAfterFilter = new ArrayList<>(4);
    for(ProxyBackendConfiguration backend: backends){
        if (!unHealthyClustersWithScheduler.contains(backend.getName())){
            backendAfterFilter.add(backend);
        }
    }
    // 为了防止出现多个注册集群指向的是同个地址，这可能导致差集为空
    if(backendAfterFilter.size() == 0){
        return routingWithClusterLoad(backends);
    }
    // 最终的兜底策略
    return routingWithClusterLoad(backendAfterFilter);
}
