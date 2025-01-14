// 请求 trino 登录页并获取token
// 具体可以参考 https://github.com/trinodb/trino/issues/3661
public static String getTrinoClusterToken(String targetUrl, MonitorConfiguration monitorConfiguration){
    URL loginUrl = new URL(String.format("%s/ui/login", targetUrl));
        loginConnection = (HttpURLConnection) loginUrl.openConnection();
        loginConnection.setRequestMethod("POST");
        loginConnection.setDoOutput(true);
        loginConnection.setInstanceFollowRedirects(false);
        loginConnection.addRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        loginConnection.setRequestProperty("charset", "utf-8");
        String password = "";
        if(!Strings.isNullOrEmpty(monitorConfiguration.getMonitorPassword())){
            password = monitorConfiguration.getMonitorPassword();
        }
        String loginData = String.format("username=%s&password=%s", monitorConfiguration.getMonitorUser(), password);
        byte[] postData = loginData.getBytes( StandardCharsets.UTF_8);
        loginConnection.connect();
        DataOutputStream out = new DataOutputStream(loginConnection.getOutputStream());
        out.write(postData);
        String token = loginConnection.getHeaderField("Set-Cookie");
        out.close();
        return token.split(";")[0];

// 根据token再请求/ui/api/stats接口获取集群负载情况
public static String requestTrinoClusterStatsWithToken(String targetUrl, String token){
URL apiURL = new URL(String.format("%s/ui/api/stats", targetUrl));
        apiConnection = (HttpURLConnection) apiURL.openConnection();
        apiConnection.setRequestMethod("GET");
        apiConnection.setRequestProperty("Cookie", token);
        int responseCode = apiConnection.getResponseCode();
        if (responseCode == HttpStatus.SC_OK) {
            BufferedReader reader = new BufferedReader(new InputStreamReader((InputStream) apiConnection.getContent()));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
            return sb.toString();
// ...
