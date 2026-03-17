import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.slx.nord.jgnmessages.JGNMessages;
import com.slx.nord.jgnmessages.clientinbound.notifications.UpdateBuildingNotificationMessage;
import com.slx.nord.jgnmessages.clientinbound.responses.CreateAccountResponseMessage;
import com.slx.nord.jgnmessages.clientinbound.responses.LoginResponseMessage;
import com.slx.nord.jgnmessages.clientinbound.responses.RequestBuildingsResponseMessage;
import com.slx.nord.jgnmessages.clientinbound.responses.RequestPlayerVillageIDsResponseMessage;
import com.slx.nord.jgnmessages.clientoutbound.CreateAccountMessage;
import com.slx.nord.jgnmessages.clientoutbound.HarvestMessage;
import com.slx.nord.jgnmessages.clientoutbound.ListenToVillageMessage;
import com.slx.nord.jgnmessages.clientoutbound.LoginMessage;
import com.slx.nord.jgnmessages.clientoutbound.RequestBuildingsMessage;
import com.slx.nord.jgnmessages.clientoutbound.RequestPlayerVillageIDsMessage;
import com.slx.nord.jgnpersistentobjectdetails.Building;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class WildResourceLifecycleSmoke {
    private static final int DEFAULT_TCP_PORT = 41210;
    private static final int DEFAULT_UDP_PORT = 41211;
    private static final long DEFAULT_CONNECT_TIMEOUT_MS = 10_000L;
    private static final long DEFAULT_MESSAGE_TIMEOUT_MS = 10_000L;
    private static final long DEFAULT_TOPUP_TIMEOUT_MS = 45_000L;
    private static final int DEFAULT_BUILD_AREA_HALF_EXTENT_TILES = 24;

    private static final class MessagePump extends Listener {
        private final LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>();

        @Override
        public void received(Connection connection, Object object) {
            if (object == null) {
                return;
            }
            String className = object.getClass().getName();
            if (className.startsWith("com.esotericsoftware.kryonet.FrameworkMessage")) {
                return;
            }
            queue.offer(object);
        }

        Object poll(long timeoutMs) throws InterruptedException {
            return queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    private static final class SmokeClient {
        final Client client;
        final MessagePump pump;

        SmokeClient(Client client, MessagePump pump) {
            this.client = client;
            this.pump = pump;
        }
    }

    private static final class Config {
        String host = "127.0.0.1";
        int tcpPort = DEFAULT_TCP_PORT;
        int udpPort = DEFAULT_UDP_PORT;
        long connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS;
        long messageTimeoutMs = DEFAULT_MESSAGE_TIMEOUT_MS;
        long topupTimeoutMs = DEFAULT_TOPUP_TIMEOUT_MS;
        boolean useUdp = false;
    }

    public static void main(String[] args) throws Exception {
        run(parseArgs(args));
    }

    private static void run(Config config) throws Exception {
        long stamp = System.currentTimeMillis();
        String username = "wild_smoke_" + stamp;
        String password = "wild_smoke_pass_" + stamp;

        int playerId;
        int villageId;

        SmokeClient createClient = connect(config);
        try {
            createClient.client.sendTCP(new CreateAccountMessage(
                username,
                password,
                "Wild",
                "Smoke",
                "wild-smoke@example.com",
                0,
                (byte) 1,
                (byte) 1,
                (short) 2000,
                (byte) 0,
                0,
                0L,
                ""
            ));

            CreateAccountResponseMessage createResponse = waitFor(
                createClient.pump,
                CreateAccountResponseMessage.class,
                config.messageTimeoutMs,
                "create account response"
            );
            ensure(createResponse.getResponseCode() == 0,
                "CreateAccount responseCode expected 0, got " + createResponse.getResponseCode());
            playerId = createResponse.getPlayerID();
            ensure(playerId > 0, "CreateAccount playerID should be >0");

            createClient.client.sendTCP(new RequestPlayerVillageIDsMessage(0, playerId));
            RequestPlayerVillageIDsResponseMessage villageIdsResponse = waitFor(
                createClient.pump,
                RequestPlayerVillageIDsResponseMessage.class,
                config.messageTimeoutMs,
                "request player village IDs response"
            );
            ArrayList ids = villageIdsResponse.getIDs();
            ensure(ids != null && !ids.isEmpty(), "Village ID list should not be empty");
            villageId = ((Number) ids.get(0)).intValue();
            ensure(villageId > 0, "Village ID should be >0");
        } finally {
            closeClient(createClient);
        }

        SmokeClient loginClient = connect(config);
        try {
            loginClient.client.sendTCP(new LoginMessage(
                username,
                password,
                "WILD-SMOKE-1",
                "WILD-SMOKE-MAC-" + stamp
            ));
            LoginResponseMessage loginResponse = waitFor(
                loginClient.pump,
                LoginResponseMessage.class,
                config.messageTimeoutMs,
                "login response"
            );
            ensure(loginResponse.getResponseCode() == 0,
                "Login responseCode expected 0, got " + loginResponse.getResponseCode());
            int sessionId = loginResponse.getSessionID();
            ensure(sessionId > 0, "Login sessionID should be >0");

            loginClient.client.sendTCP(new ListenToVillageMessage(sessionId, villageId));
            sleepQuietly(300L);

            ArrayList<Building> baselineBuildings =
                requestBuildings(loginClient, sessionId, villageId, config.messageTimeoutMs);
            short resourceType = detectDominantWildResourceType(baselineBuildings);
            Building target = selectHarvestTarget(baselineBuildings, resourceType);
            int baselineCount = countActiveWildResources(baselineBuildings, resourceType);
            ensure(baselineCount > 0, "Baseline wild resource count should be >0");

            loginClient.client.sendTCP(new HarvestMessage(
                sessionId,
                villageId,
                target.getBuildingID(),
                false,
                0L
            ));

            waitForMatching(
                loginClient.pump,
                UpdateBuildingNotificationMessage.class,
                config.messageTimeoutMs,
                "wild resource removal update",
                value ->
                    value.getBuildingID() == target.getBuildingID() &&
                    !value.isReady() &&
                    !value.isPlacedOnMap() &&
                    value.isConsumed()
            );

            ArrayList<Building> afterHarvestBuildings =
                requestBuildings(loginClient, sessionId, villageId, config.messageTimeoutMs);
            int afterHarvestCount = countActiveWildResources(afterHarvestBuildings, resourceType);
            ensure(afterHarvestCount <= baselineCount - 1,
                "Wild resource count should drop after harvest. baseline=" + baselineCount +
                    " afterHarvest=" + afterHarvestCount);

            int afterTopupCount = waitForWildResourceTopUp(
                loginClient,
                sessionId,
                villageId,
                resourceType,
                baselineCount,
                config.messageTimeoutMs,
                config.topupTimeoutMs
            );
            ensure(afterTopupCount >= baselineCount,
                "Wild resource count did not recover after top-up. baseline=" + baselineCount +
                    " afterTopup=" + afterTopupCount);

            System.out.println(
                "WILD_RESOURCE_SMOKE_PASS username=" + username +
                    " villageId=" + villageId +
                    " resourceType=" + resourceType +
                    " baselineCount=" + baselineCount +
                    " afterHarvestCount=" + afterHarvestCount +
                    " afterTopupCount=" + afterTopupCount
            );
        } finally {
            closeClient(loginClient);
        }
    }

    private static ArrayList<Building> requestBuildings(SmokeClient client,
                                                        int sessionId,
                                                        int villageId,
                                                        long timeoutMs) throws Exception {
        client.client.sendTCP(new RequestBuildingsMessage(sessionId, villageId));
        RequestBuildingsResponseMessage response = waitForMatching(
            client.pump,
            RequestBuildingsResponseMessage.class,
            timeoutMs,
            "request buildings response",
            value -> value.getVillageID() == villageId
        );
        ArrayList<Building> buildings = response.getBuildings();
        ensure(buildings != null && !buildings.isEmpty(), "Buildings payload should not be empty");
        return buildings;
    }

    private static short detectDominantWildResourceType(ArrayList<Building> buildings) {
        Map<Short, Integer> activeCountsByType = new HashMap<>();
        for (Building building : buildings) {
            if (!isActiveWildResourceCandidate(building)) {
                continue;
            }
            short type = building.getBuildingType();
            activeCountsByType.put(type, activeCountsByType.getOrDefault(type, 0) + 1);
        }
        short selectedType = 0;
        int selectedCount = -1;
        for (Map.Entry<Short, Integer> entry : activeCountsByType.entrySet()) {
            short type = entry.getKey();
            int count = entry.getValue();
            if (count > selectedCount || (count == selectedCount && type < selectedType)) {
                selectedType = type;
                selectedCount = count;
            }
        }
        ensure(selectedType > 0 && selectedCount > 0,
            "Failed to detect dominant active wild resource type from buildings payload");
        return selectedType;
    }

    private static Building selectHarvestTarget(ArrayList<Building> buildings, short resourceType) {
        for (Building building : buildings) {
            if (!isActiveWildResourceCandidate(building)) {
                continue;
            }
            if (building.getBuildingType() == resourceType) {
                return building;
            }
        }
        throw new IllegalStateException("No harvest target found for resourceType=" + resourceType);
    }

    private static int countActiveWildResources(ArrayList<Building> buildings, short resourceType) {
        int count = 0;
        for (Building building : buildings) {
            if (building == null || building.getBuildingType() != resourceType) {
                continue;
            }
            if (building.isReady() && building.isPlacedOnMap() && !building.isConsumed()) {
                count++;
            }
        }
        return count;
    }

    private static boolean isActiveWildResourceCandidate(Building building) {
        if (building == null) {
            return false;
        }
        if (!building.isReady() || !building.isPlacedOnMap() || building.isConsumed()) {
            return false;
        }
        return isOutsideBuildArea(building.getTileX(), building.getTileZ());
    }

    private static boolean isOutsideBuildArea(byte tileX, byte tileZ) {
        return Math.abs((int) tileX) > DEFAULT_BUILD_AREA_HALF_EXTENT_TILES ||
            Math.abs((int) tileZ) > DEFAULT_BUILD_AREA_HALF_EXTENT_TILES;
    }

    private static int waitForWildResourceTopUp(SmokeClient client,
                                                int sessionId,
                                                int villageId,
                                                short resourceType,
                                                int baselineCount,
                                                long messageTimeoutMs,
                                                long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + Math.max(1L, timeoutMs);
        int latestCount = 0;
        while (System.currentTimeMillis() < deadline) {
            ArrayList<Building> current = requestBuildings(client, sessionId, villageId, messageTimeoutMs);
            latestCount = countActiveWildResources(current, resourceType);
            if (latestCount >= baselineCount) {
                return latestCount;
            }
            sleepQuietly(1000L);
        }
        throw new IllegalStateException(
            "Timed out waiting for wild resource top-up. baseline=" + baselineCount +
                " latest=" + latestCount +
                " timeoutMs=" + timeoutMs
        );
    }

    private static SmokeClient connect(Config config) throws IOException, InterruptedException {
        int maxAttempts = config.useUdp ? 3 : 1;
        IOException lastIOException = null;
        RuntimeException lastRuntimeException = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            Client client = new Client(524_288, 524_288);
            JGNMessages.registerJGNMessageClasses(client.getKryo());
            MessagePump pump = new MessagePump();
            client.addListener(pump);
            client.start();
            int connectTimeout = (int) Math.max(1, Math.min(Integer.MAX_VALUE, config.connectTimeoutMs));
            try {
                if (config.useUdp) {
                    client.connect(connectTimeout, config.host, config.tcpPort, config.udpPort);
                } else {
                    client.connect(connectTimeout, config.host, config.tcpPort);
                }
                return new SmokeClient(client, pump);
            } catch (IOException e) {
                lastIOException = e;
                closeClient(new SmokeClient(client, pump));
            } catch (RuntimeException e) {
                lastRuntimeException = e;
                closeClient(new SmokeClient(client, pump));
            }
            if (attempt < maxAttempts) {
                Thread.sleep(250L * attempt);
            }
        }
        if (lastIOException != null) {
            throw lastIOException;
        }
        if (lastRuntimeException != null) {
            throw lastRuntimeException;
        }
        throw new IOException("Unable to connect smoke client");
    }

    private static void closeClient(SmokeClient smokeClient) {
        if (smokeClient == null || smokeClient.client == null) {
            return;
        }
        try {
            smokeClient.client.close();
        } catch (Exception ignored) {
        }
        try {
            smokeClient.client.stop();
        } catch (Exception ignored) {
        }
    }

    private static <T> T waitFor(MessagePump pump,
                                 Class<T> messageType,
                                 long timeoutMs,
                                 String label) throws Exception {
        return waitForMatching(pump, messageType, timeoutMs, label, value -> true);
    }

    private static <T> T waitForMatching(MessagePump pump,
                                         Class<T> messageType,
                                         long timeoutMs,
                                         String label,
                                         Predicate<T> predicate) throws Exception {
        long deadline = System.currentTimeMillis() + Math.max(1L, timeoutMs);
        while (System.currentTimeMillis() < deadline) {
            long remaining = deadline - System.currentTimeMillis();
            Object message = pump.poll(Math.min(remaining, 1000L));
            if (message == null) {
                continue;
            }
            if (!messageType.isInstance(message)) {
                continue;
            }
            T typed = messageType.cast(message);
            if (predicate.test(typed)) {
                return typed;
            }
        }
        throw new IllegalStateException("Timed out waiting for " + label + " (" + messageType.getSimpleName() + ")");
    }

    private static void ensure(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static void sleepQuietly(long millis) {
        if (millis <= 0L) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    private static Config parseArgs(String[] args) {
        Config config = new Config();
        if (args == null) {
            return config;
        }
        for (String arg : args) {
            if (arg == null) {
                continue;
            }
            String trimmed = arg.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            if ("--use-udp".equals(trimmed)) {
                config.useUdp = true;
                continue;
            }
            if (trimmed.startsWith("--host=")) {
                config.host = trimmed.substring("--host=".length());
                continue;
            }
            if (trimmed.startsWith("--tcp-port=")) {
                config.tcpPort = parseIntOrDefault(trimmed.substring("--tcp-port=".length()), config.tcpPort);
                continue;
            }
            if (trimmed.startsWith("--udp-port=")) {
                config.udpPort = parseIntOrDefault(trimmed.substring("--udp-port=".length()), config.udpPort);
                continue;
            }
            if (trimmed.startsWith("--connect-timeout-ms=")) {
                config.connectTimeoutMs = parseLongOrDefault(trimmed.substring("--connect-timeout-ms=".length()), config.connectTimeoutMs);
                continue;
            }
            if (trimmed.startsWith("--message-timeout-ms=")) {
                config.messageTimeoutMs = parseLongOrDefault(trimmed.substring("--message-timeout-ms=".length()), config.messageTimeoutMs);
                continue;
            }
            if (trimmed.startsWith("--topup-timeout-ms=")) {
                config.topupTimeoutMs = parseLongOrDefault(trimmed.substring("--topup-timeout-ms=".length()), config.topupTimeoutMs);
            }
        }
        return config;
    }

    private static int parseIntOrDefault(String value, int fallback) {
        try {
            return Integer.parseInt(value);
        } catch (RuntimeException ignored) {
            return fallback;
        }
    }

    private static long parseLongOrDefault(String value, long fallback) {
        try {
            return Long.parseLong(value);
        } catch (RuntimeException ignored) {
            return fallback;
        }
    }
}
