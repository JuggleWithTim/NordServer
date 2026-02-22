import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.esotericsoftware.minlog.Log;
import com.slx.nord.jgnmessages.JGNMessages;
import com.slx.nord.jgnmessages.clientinbound.notifications.*;
import com.slx.nord.jgnmessages.clientinbound.responses.*;
import com.slx.nord.jgnmessages.clientoutbound.*;
import com.slx.nord.jgnpersistentobjectdetails.AvatarDetails;
import com.slx.nord.jgnpersistentobjectdetails.Building;
import com.slx.nord.jgnpersistentobjectdetails.Tree;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class NordServer {
    private static final int TCP_PORT = 41210;
    private static final int UDP_PORT = 41211;

    private static final int LOGIN_FAILED_RESPONSE_CODE = 1;
    private static final int CREATE_ACCOUNT_USERNAME_TAKEN = 1;
    private static final int CREATE_ACCOUNT_INVALID_USERNAME = 2;
    private static final String DEFAULT_DB_PATH = "nord-data.sqlite";
    private static final String DEFAULT_BUILDING_COSTS_PATH = "building-costs.properties";

    private static final byte[] DEFAULT_HEIGHTMAP = buildDefaultHeightMap();
    private static final int DEFAULT_SERVER_BUILDING_COST = resolveDefaultBuildingCost();
    private static final Map<Short, Integer> SERVER_BUILDING_COST_OVERRIDES = loadBuildingCostOverrides();

    private static final Map<Integer, Connection> CONNECTIONS_BY_ID = new ConcurrentHashMap<>();
    private static final Map<Integer, NordDatabase.UserRecord> USERS_BY_CONNECTION = new ConcurrentHashMap<>();
    private static final Map<Integer, Integer> ACTIVE_VILLAGE_BY_CONNECTION = new ConcurrentHashMap<>();
    private static final Map<Integer, Map<Integer, Connection>> VILLAGE_CONNECTIONS = new ConcurrentHashMap<>();
    private static final Map<Integer, AvatarPosition> AVATAR_POSITIONS_BY_PLAYER = new ConcurrentHashMap<>();
    private static final Map<String, Integer> UNHANDLED_MESSAGE_COUNTS = new ConcurrentHashMap<>();

    private static NordDatabase DATABASE;
    private static boolean UDP_ENABLED = true;

    private static byte[] buildDefaultHeightMap() {
        int size = 129;
        byte[] map = new byte[size * size];
        int cx = size / 2;
        int cy = size / 2;
        int landRadius = 52;
        int flatRadius = 40;
        int landHeight = 10;
        for (int y = 0; y < size; y++) {
            int dy = y - cy;
            for (int x = 0; x < size; x++) {
                int dx = x - cx;
                int dist2 = dx * dx + dy * dy;
                int idx = y * size + x;
                if (dist2 <= landRadius * landRadius) {
                    int dist = (int) Math.sqrt(dist2);
                    int h;
                    if (dist <= flatRadius) {
                        h = landHeight;
                    } else {
                        int edge = landRadius - flatRadius;
                        int t = dist - flatRadius;
                        h = landHeight - (t * landHeight / edge);
                    }
                    if (h < 1) h = 1;
                    map[idx] = (byte) h;
                } else {
                    map[idx] = 0;
                }
            }
        }
        return map;
    }

    public static void main(String[] args) throws IOException {
        String databasePath = resolveDatabasePath();
        DATABASE = new NordDatabase(databasePath);

        Log.set(Log.LEVEL_INFO);
        Server server = new Server(524288, 524288);
        Kryo kryo = server.getKryo();
        JGNMessages.registerJGNMessageClasses(kryo);

        server.addListener(new Listener() {
            @Override
            public void connected(Connection connection) {
                CONNECTIONS_BY_ID.put(connection.getID(), connection);
                log("[Server] Client connected: " + connection.getRemoteAddressTCP());
            }

            @Override
            public void disconnected(Connection connection) {
                handleDisconnect(connection);
                log("[Server] Client disconnected: " + connection.getRemoteAddressTCP());
            }

            @Override
            public void received(Connection connection, Object object) {
                try {
                    handleMessage(connection, object);
                } catch (Exception e) {
                    System.out.println("[Server] Error handling message: " + object.getClass().getName());
                    e.printStackTrace();
                }
            }
        });

        boolean enableUdp = true;
        for (String arg : args) {
            if ("--udp".equalsIgnoreCase(arg)) {
                enableUdp = true;
                continue;
            }
            if ("--tcp-only".equalsIgnoreCase(arg) || "--no-udp".equalsIgnoreCase(arg)) {
                enableUdp = false;
            }
        }
        UDP_ENABLED = enableUdp;

        if (enableUdp) {
            server.bind(TCP_PORT, UDP_PORT);
            log("Nord standalone server listening on TCP " + TCP_PORT + " / UDP " + UDP_PORT);
        } else {
            server.bind(TCP_PORT);
            log("Nord standalone server listening on TCP " + TCP_PORT + " (UDP disabled)");
        }
        log("Database path: " + databasePath + " (users=" + DATABASE.getUserCount() + ")");
        log("Building cost config: default=" + DEFAULT_SERVER_BUILDING_COST + ", overrides=" + SERVER_BUILDING_COST_OVERRIDES.size());
        server.start();
    }

    private static void handleMessage(Connection connection, Object msg) {
        if (msg == null) return;
        if (msg.getClass().getName().startsWith("com.esotericsoftware.kryonet.FrameworkMessage")) {
            return;
        }

        log("[Server] Received: " + msg.getClass().getName());

        if (msg instanceof com.slx.nord.jgnmessages.clientinbound.GenericByteArrayMessage) {
            com.slx.nord.jgnmessages.clientinbound.GenericByteArrayMessage m =
                (com.slx.nord.jgnmessages.clientinbound.GenericByteArrayMessage) msg;
            log("[Server] GenericByteArrayMessage type=" + m.getType() + " bytes=" + (m.getData() == null ? 0 : m.getData().length));
            // Never deserialize untrusted embedded payloads.
            return;
        }

        if (msg instanceof LoginMessage) {
            LoginMessage m = (LoginMessage) msg;
            NordDatabase.UserRecord user = DATABASE.authenticate(safe(m.getUsername()), safe(m.getPassword()));
            if (user == null) {
                sendLoginFailure(connection);
                return;
            }
            USERS_BY_CONNECTION.put(connection.getID(), user);
            sendLoginSuccess(connection, user);
            sendAchievementSnapshot(connection, user.userId);
            sendBlockedPlayersSnapshot(connection, user.userId);
            sendIMListSnapshot(connection, user.userId);
            return;
        }

        if (msg instanceof FacebookLoginMessage) {
            // Social login is disabled by default because the legacy protocol does not verify provider tokens.
            sendLoginFailure(connection);
            return;
        }

        if (msg instanceof CreateAccountMessage) {
            CreateAccountMessage m = (CreateAccountMessage) msg;
            String username = safe(m.getUserName());
            NordDatabase.CreateUserResult result = createUser(username, safe(m.getPassword()));
            if (!result.success) {
                int responseCode = result.usernameTaken ? CREATE_ACCOUNT_USERNAME_TAKEN : CREATE_ACCOUNT_INVALID_USERNAME;
                connection.sendTCP(new CreateAccountResponseMessage(responseCode, 0, false, 0));
                return;
            }
            USERS_BY_CONNECTION.put(connection.getID(), result.user);
            connection.sendTCP(new CreateAccountResponseMessage(0, result.user.userId, true, 0));
            return;
        }

        if (msg instanceof RequestServerTimeMessage) {
            connection.sendTCP(new RequestServerTimeResponseMessage(Instant.now().toEpochMilli()));
            return;
        }

        if (msg instanceof KeepAliveMessage) {
            connection.sendTCP(new KeepAliveResponseMessage());
            return;
        }

        if (msg instanceof OutgoingUDPTestMessage) {
            connection.sendUDP(new IncomingUDPTestMessage());
            return;
        }

        if (msg instanceof UDPEnabledMessage) {
            return;
        }

        NordDatabase.UserRecord authenticatedUser = USERS_BY_CONNECTION.get(connection.getID());
        if (authenticatedUser == null) {
            log("[Server] Rejected unauthenticated message: " + msg.getClass().getName());
            return;
        }

        if (msg instanceof RequestVillageNameMessage) {
            RequestVillageNameMessage m = (RequestVillageNameMessage) msg;
            NordDatabase.UserRecord user = DATABASE.findByVillageId(m.getVillageID());
            String name = user != null ? user.villageName : "Village " + m.getVillageID();
            connection.sendTCP(new RequestVillageNameResponseMessage(m.getVillageID(), name));
            return;
        }

        if (msg instanceof RequestPlayerNameMessage) {
            RequestPlayerNameMessage m = (RequestPlayerNameMessage) msg;
            NordDatabase.UserRecord user = DATABASE.findByUserId(m.getPlayerID());
            String name = user != null ? user.username : "Player " + m.getPlayerID();
            connection.sendTCP(new RequestPlayerNameResponseMessage(m.getPlayerID(), name));
            return;
        }

        if (msg instanceof RequestPlayerVillageIDsMessage) {
            RequestPlayerVillageIDsMessage m = (RequestPlayerVillageIDsMessage) msg;
            int playerId = resolveReadablePlayerId(connection, m.getPlayerID());
            NordDatabase.UserRecord user = DATABASE.findByUserId(playerId);
            ArrayList<Integer> ids = new ArrayList<>();
            if (user != null) ids.add(user.villageId);
            connection.sendTCP(new RequestPlayerVillageIDsResponseMessage(playerId, ids));
            return;
        }

        if (msg instanceof RequestHeightMapMessage) {
            RequestHeightMapMessage m = (RequestHeightMapMessage) msg;
            int villageId = resolveVillageId(connection, m.getVillageID());
            ensureVillageSubscription(connection, villageId, "RequestHeightMap");
            connection.sendTCP(new RequestHeightMapResponseMessage(villageId, Arrays.copyOf(DEFAULT_HEIGHTMAP, DEFAULT_HEIGHTMAP.length), (byte) 0, (byte) 0));
            return;
        }

        if (msg instanceof RequestBuildingsMessage) {
            RequestBuildingsMessage m = (RequestBuildingsMessage) msg;
            int villageId = resolveVillageId(connection, m.getVillageID());
            ensureVillageSubscription(connection, villageId, "RequestBuildings");
            ArrayList<Building> buildings = DATABASE.loadBuildings(villageId);
            connection.sendTCP(new RequestBuildingsResponseMessage(villageId, buildings, (byte) 1, (byte) 1, 0));
            return;
        }

        if (msg instanceof AddBuildingMessage) {
            AddBuildingMessage m = (AddBuildingMessage) msg;
            int villageId = resolveLiveVillageId(connection, m.getVillageID());
            if (villageId != 0 && !isConnectionInVillage(connection, villageId)) {
                joinVillage(connection, villageId);
            }
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            Building building = m.getBuilding();
            Building existing = building != null ? findBuilding(villageId, building.getBuildingID()) : null;
            boolean isNewPlacement = building != null && existing == null;
            boolean ownVillagePlacement = user != null && villageId == user.villageId && isNewPlacement;
            int explicitExtraCost = resolveAddBuildingCreditsDelta(m);
            int creditsDelta = explicitExtraCost;
            if (ownVillagePlacement) {
                int uiBuildingCost = resolveClientDisplayedBuildingCost(building);
                int totalCost = uiBuildingCost + explicitExtraCost;
                creditsDelta = totalCost > 0 ? -totalCost : 0;
            }
            if (user != null && creditsDelta != 0) {
                try {
                    if (creditsDelta < 0) {
                        int current = DATABASE.getSLXCredits(user.userId);
                        if (current < Math.abs(creditsDelta)) {
                            connection.sendTCP(new RequestSLXCreditsResponseMessage(current));
                            try {
                                short buildingId = building != null ? building.getBuildingID() : 0;
                                connection.sendTCP(com.slx.nord.jgnmessages.clientinbound.GenericByteArrayMessage.createSerializedByteArrayMessage(
                                    new com.slx.nord.jgnmessages.bytearraymessages.BuildingNotAddedNotEnoughCreditsMessage(villageId, buildingId)
                                ));
                            } catch (Exception ignored) {
                                // Optional compatibility signal, ignore if serialization fails.
                            }
                            log("[Server] AddBuilding denied (insufficient credits) userId=" + user.userId +
                                " required=" + Math.abs(creditsDelta) + " current=" + current);
                            return;
                        }
                    }
                    int amount = DATABASE.applySLXCreditDelta(user.userId, creditsDelta);
                    refreshCachedUserRecord(user.userId);
                    connection.sendTCP(new RequestSLXCreditsResponseMessage(amount));
                    log("[Server] AddBuilding credits delta applied userId=" + user.userId + " delta=" + creditsDelta + " newBalance=" + amount);
                } catch (IOException e) {
                    log("[Server] Failed to apply building credits delta: " + e.getMessage());
                }
            }
            if (user != null) {
                log("[Server] AddBuilding received userId=" + user.userId + " buildingId=" +
                    (building != null ? building.getBuildingID() : -1) +
                    " extraCredits=" + m.getExtraCredits() + " extraMessage=\"" + safe(m.getExtraMessage()) + "\"");
            }
            if (building != null) {
                try {
                    DATABASE.upsertBuilding(villageId, building);
                } catch (IOException e) {
                    log("[Server] Failed to store building: " + e.getMessage());
                }
                broadcastToVillage(villageId, new BuildingAddedNotificationMessage(villageId, building), null, true, false);
            }
            return;
        }

        if (msg instanceof UpdateBuildingMessage) {
            UpdateBuildingMessage m = (UpdateBuildingMessage) msg;
            int villageId = resolveLiveVillageId(connection, m.getVillageID());
            if (villageId != 0 && !isConnectionInVillage(connection, villageId)) {
                joinVillage(connection, villageId);
            }
            try {
                DATABASE.applyBuildingUpdate(
                    villageId,
                    m.getBuildingID(),
                    m.getData(),
                    m.isConsumed(),
                    m.isPlacedOnMap(),
                    m.getTileX(),
                    m.getTileZ(),
                    m.getRotation(),
                    m.isReady(),
                    m.getDelayStart(),
                    m.getDelayEnd(),
                    m.getIngredients()
                );
            } catch (IOException e) {
                log("[Server] Failed to update building: " + e.getMessage());
            }
            broadcastToVillage(villageId, new UpdateBuildingNotificationMessage(
                villageId,
                m.getBuildingID(),
                safeBytes(m.getData()),
                m.isConsumed(),
                m.isPlacedOnMap(),
                m.getTileX(),
                m.getTileZ(),
                m.getRotation(),
                m.isReady(),
                m.getDelayStart(),
                m.getDelayEnd(),
                m.getIngredients()
            ), null, true, false);
            return;
        }

        if (msg instanceof RemoveBuildingMessage) {
            RemoveBuildingMessage m = (RemoveBuildingMessage) msg;
            int villageId = resolveLiveVillageId(connection, m.getVillageID());
            if (villageId != 0 && !isConnectionInVillage(connection, villageId)) {
                joinVillage(connection, villageId);
            }
            try {
                DATABASE.removeBuilding(villageId, m.getBuildingID());
            } catch (IOException e) {
                log("[Server] Failed to remove building: " + e.getMessage());
            }
            broadcastToVillage(villageId, new BuildingRemovedNotificationMessage(villageId, m.getBuildingID()), null, true, false);
            return;
        }

        if (msg instanceof RequestTreesMessage) {
            RequestTreesMessage m = (RequestTreesMessage) msg;
            int villageId = resolveVillageId(connection, m.getVillageID());
            ensureVillageSubscription(connection, villageId, "RequestTrees");
            connection.sendTCP(new RequestTreesResponseMessage(villageId, DATABASE.loadTrees(villageId)));
            return;
        }

        if (msg instanceof StoreTreesMessage) {
            StoreTreesMessage m = (StoreTreesMessage) msg;
            int villageId = resolveLiveVillageId(connection, m.getVillageID());
            if (villageId != 0 && !isConnectionInVillage(connection, villageId)) {
                joinVillage(connection, villageId);
            }
            try {
                DATABASE.upsertTrees(villageId, m.getTrees());
            } catch (IOException e) {
                log("[Server] Failed to store trees: " + e.getMessage());
            }
            broadcastToVillage(villageId, new TreesStoredNotificationMessage(villageId, m.getTrees()), null, true, false);
            return;
        }

        if (msg instanceof RemoveTreesMessage) {
            RemoveTreesMessage m = (RemoveTreesMessage) msg;
            int villageId = resolveLiveVillageId(connection, m.getVillageID());
            if (villageId != 0 && !isConnectionInVillage(connection, villageId)) {
                joinVillage(connection, villageId);
            }
            try {
                DATABASE.removeTrees(villageId, m.getTrees());
            } catch (IOException e) {
                log("[Server] Failed to remove trees: " + e.getMessage());
            }
            broadcastToVillage(villageId, new TreesRemovedNotificationMessage(villageId, m.getTrees()), null, true, false);
            return;
        }

        if (msg instanceof RequestCharactersMessage) {
            RequestCharactersMessage m = (RequestCharactersMessage) msg;
            int villageId = resolveVillageId(connection, m.getVillageID());
            ensureVillageSubscription(connection, villageId, "RequestCharacters");
            connection.sendTCP(new RequestCharactersResponseMessage(villageId, new ArrayList<>()));
            return;
        }

        if (msg instanceof RequestOnlinePlayersMessage) {
            PlayerDirectoryData online = buildPlayerDirectoryData(false, 0);
            connection.sendTCP(new RequestOnlinePlayersResponseMessage(
                online.names,
                online.villageIDs,
                online.userIDs,
                online.ages,
                online.sexes,
                online.countries,
                online.cities,
                online.scores,
                online.photoURLs,
                online.villageNames,
                online.visitors,
                online.themes,
                online.levels
            ));
            return;
        }

        if (msg instanceof StopListeningToVillageMessage) {
            // Ignore explicit stop-listen while running with a single active village stream.
            // Village switches are handled by the next join request (listen/play/enter).
            return;
        }

        if (msg instanceof StopPlayingWithVillageMessage) {
            // This message has no village identifier and can arrive out-of-order while switching villages.
            // Avoid tearing down the latest village subscription by mistake.
            return;
        }

        if (msg instanceof LogoutMessage) {
            logoutConnection(connection);
            return;
        }

        if (msg instanceof ListenToVillageMessage) {
            ListenToVillageMessage m = (ListenToVillageMessage) msg;
            int villageId = resolveVillageId(connection, m.getVillageID());
            joinVillage(connection, villageId);
            return;
        }

        if (msg instanceof PlayWithVillageMessage) {
            PlayWithVillageMessage m = (PlayWithVillageMessage) msg;
            int villageId = resolveVillageId(connection, m.getVillageID());
            joinVillage(connection, villageId);
            return;
        }

        if (msg instanceof EnterVillageWithAvatarMessage) {
            EnterVillageWithAvatarMessage m = (EnterVillageWithAvatarMessage) msg;
            int villageId = resolveVillageId(connection, m.getVillageID());
            joinVillage(connection, villageId);
            return;
        }

        if (msg instanceof LeaveVillageWithAvatarMessage) {
            leaveVillageAndReturnHome(connection);
            return;
        }

        if (msg instanceof MoveAvatarMessage) {
            MoveAvatarMessage m = (MoveAvatarMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                return;
            }
            int villageId = resolveLiveVillageId(connection, m.getVillageID());
            if (villageId == 0) {
                return;
            }
            if (!isConnectionInVillage(connection, villageId)) {
                joinVillage(connection, villageId);
            }
            MoveAvatarNotificationMessage notification = new MoveAvatarNotificationMessage(
                villageId,
                user.userId,
                m.getPosX(),
                m.getPosZ(),
                m.getRotation()
            );
            AVATAR_POSITIONS_BY_PLAYER.put(user.userId, new AvatarPosition(villageId, m.getPosX(), m.getPosZ(), m.getRotation()));
            broadcastToVillage(villageId, notification, connection, false, UDP_ENABLED);
            return;
        }

        if (msg instanceof AvatarEventMessage) {
            AvatarEventMessage m = (AvatarEventMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                return;
            }
            int villageId = resolveLiveVillageId(connection, m.getVillageID());
            if (villageId != 0 && !isConnectionInVillage(connection, villageId)) {
                joinVillage(connection, villageId);
            }
            broadcastToVillage(
                villageId,
                new AvatarEventNotificationMessage(villageId, user.userId, m.getEventID(), safeBytes(m.getData())),
                null,
                true,
                false
            );
            return;
        }

        if (msg instanceof CharacterEventMessage) {
            CharacterEventMessage m = (CharacterEventMessage) msg;
            int villageId = resolveLiveVillageId(connection, m.getVillageID());
            if (villageId != 0 && !isConnectionInVillage(connection, villageId)) {
                joinVillage(connection, villageId);
            }
            broadcastToVillage(
                villageId,
                new CharacterEventNotificationMessage(villageId, m.getCharacterID(), m.getEventID(), safeBytes(m.getData())),
                null,
                true,
                false
            );
            return;
        }

        if (msg instanceof HarvestMessage) {
            HarvestMessage m = (HarvestMessage) msg;
            int villageId = resolveLiveVillageId(connection, m.getVillageID());
            if (villageId != 0 && !isConnectionInVillage(connection, villageId)) {
                joinVillage(connection, villageId);
            }
            Building existing = findBuilding(villageId, m.getNetworkIdentificationID());
            if (existing == null) {
                return;
            }
            long now = System.currentTimeMillis();
            if (!existing.isReady()) {
                return;
            }
            if (existing.getDelayEnd() > now) {
                // Ignore duplicate/early harvest requests while crop is still on cooldown.
                return;
            }
            applyBuildingStateDelta(
                villageId,
                m.getNetworkIdentificationID(),
                null,
                null,
                null,
                Boolean.FALSE,
                m.isWriteDelayStart(),
                m.getDelayEndOffset(),
                0L
            );
            return;
        }

        if (msg instanceof UpdateIngredientsMessage) {
            UpdateIngredientsMessage m = (UpdateIngredientsMessage) msg;
            int villageId = resolveLiveVillageId(connection, m.getVillageID());
            if (villageId != 0 && !isConnectionInVillage(connection, villageId)) {
                joinVillage(connection, villageId);
            }
            Building building = findBuilding(villageId, m.getNetworkIdentificationID());
            long currentIngredients = building != null ? building.getIngredients() : 0L;
            long updatedIngredients;
            switch (m.getMethod()) {
                case UpdateIngredientsMessage.METHOD_SET:
                    updatedIngredients = m.getIngredients();
                    break;
                case UpdateIngredientsMessage.METHOD_ADD:
                    updatedIngredients = currentIngredients + m.getIngredients();
                    break;
                case UpdateIngredientsMessage.METHOD_OR:
                default:
                    updatedIngredients = currentIngredients | m.getIngredients();
                    break;
            }
            applyBuildingStateDelta(
                villageId,
                m.getNetworkIdentificationID(),
                null,
                null,
                null,
                null,
                m.isWriteDelayStart(),
                m.getDelayEndOffset(),
                updatedIngredients
            );
            return;
        }

        if (msg instanceof ConsumeMessage) {
            ConsumeMessage m = (ConsumeMessage) msg;
            int villageId = resolveLiveVillageId(connection, m.getVillageID());
            if (villageId != 0 && !isConnectionInVillage(connection, villageId)) {
                joinVillage(connection, villageId);
            }
            applyBuildingStateDelta(
                villageId,
                m.getNetworkIdentificationID(),
                null,
                Boolean.TRUE,
                null,
                Boolean.FALSE,
                false,
                0L,
                null
            );
            return;
        }

        if (msg instanceof SendTextToVillageMessage) {
            SendTextToVillageMessage m = (SendTextToVillageMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                return;
            }
            int villageId = resolveLiveVillageId(connection, m.getVillageID());
            if (villageId != 0 && !isConnectionInVillage(connection, villageId)) {
                joinVillage(connection, villageId);
            }
            SendTextToVillageNotificationMessage notification = new SendTextToVillageNotificationMessage(
                villageId,
                user.userId,
                user.username,
                safe(m.getWhatToSay())
            );
            broadcastToVillage(villageId, notification, null, true, false);
            return;
        }

        if (msg instanceof SendTextToPlayerMessage) {
            SendTextToPlayerMessage m = (SendTextToPlayerMessage) msg;
            NordDatabase.UserRecord fromUser = USERS_BY_CONNECTION.get(connection.getID());
            if (fromUser == null) {
                return;
            }
            Connection toConnection = findConnectionByPlayerId(m.getPlayerID());
            if (toConnection != null) {
                toConnection.sendTCP(new SendTextToPlayerNotificationMessage(fromUser.userId, fromUser.username, safe(m.getWhatToSay())));
            }
            return;
        }

        if (msg instanceof RequestIMListMessage) {
            RequestIMListMessage m = (RequestIMListMessage) msg;
            int playerId = resolvePlayerId(connection, m.getPlayerID());
            sendIMListSnapshot(connection, playerId);
            return;
        }

        if (msg instanceof RequestVillageVisitorsMessage) {
            RequestVillageVisitorsMessage m = (RequestVillageVisitorsMessage) msg;
            int villageId = resolveVillageId(connection, m.getVillageID());
            PlayerDirectoryData visitors = buildPlayerDirectoryData(true, villageId);
            connection.sendTCP(new RequestVillageVisitorsResponseMessage(
                villageId,
                visitors.names,
                visitors.villageIDs,
                visitors.userIDs,
                visitors.ages,
                visitors.sexes,
                visitors.countries,
                visitors.cities,
                visitors.onlines,
                visitors.photoURLs,
                visitors.villageNames,
                visitors.visitors,
                visitors.themes,
                visitors.levels
            ));
            return;
        }

        if (msg instanceof RequestBlockedPlayersMessage) {
            RequestBlockedPlayersMessage m = (RequestBlockedPlayersMessage) msg;
            int playerId = resolvePlayerId(connection, m.getPlayerID());
            sendBlockedPlayersSnapshot(connection, playerId);
            return;
        }

        if (msg instanceof RequestGuestbookEntriesMessage) {
            RequestGuestbookEntriesMessage m = (RequestGuestbookEntriesMessage) msg;
            int playerId = resolveReadablePlayerId(connection, m.getPlayerID());
            NordDatabase.UserRecord owner = DATABASE.findByUserId(playerId);
            String ownerName = owner != null ? owner.username : ("Player " + playerId);
            connection.sendTCP(new RequestGuestbookEntriesResponseMessage(
                false,
                false,
                ownerName,
                "",
                0,
                (byte) 0,
                playerId,
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>()
            ));
            return;
        }

        if (msg instanceof RequestGuestbookHistory) {
            RequestGuestbookHistory m = (RequestGuestbookHistory) msg;
            connection.sendTCP(new RequestGuestbookHistoryResponseMessage(
                false,
                false,
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                m.getOffset(),
                m.getPlayer1(),
                m.getPlayer2(),
                new ArrayList<>(),
                new ArrayList<>()
            ));
            return;
        }

        if (msg instanceof RequestImageMessage) {
            RequestImageMessage m = (RequestImageMessage) msg;
            connection.sendTCP(new RequestImageResponseMessage(m.getImageId(), m.isThumbnail(), new byte[0]));
            return;
        }

        if (msg instanceof RequestBadgeDataMessage) {
            RequestBadgeDataMessage m = (RequestBadgeDataMessage) msg;
            int playerId = resolveReadablePlayerId(connection, m.getPlayerID());
            com.nordclientpc.eM emptyBadgeData = new com.nordclientpc.eM(new ConcurrentHashMap<>(), false);
            connection.sendTCP(new RequestBadgeDataResponseMessage(playerId, emptyBadgeData));
            return;
        }

        if (msg instanceof RequestMidiMessage) {
            RequestMidiMessage m = (RequestMidiMessage) msg;
            int playerId = resolvePlayerId(connection, 0);
            connection.sendTCP(new RequestMidiResponseMessage(playerId, m.getSongID(), new byte[0], false));
            return;
        }

        if (msg instanceof RequestPointWalkQuestionStatisticsMessage) {
            RequestPointWalkQuestionStatisticsMessage m = (RequestPointWalkQuestionStatisticsMessage) msg;
            int villageId = resolveVillageId(connection, m.getVillageID());
            connection.sendTCP(new RequestPointWalkQuestionStatisticsResponseMessage(
                villageId,
                m.getBuildingThatContainsQuestionID(),
                0,
                0,
                0,
                0,
                0
            ));
            return;
        }

        if (msg instanceof RequestPointWalkVillageAnswersForPlayerMessage) {
            RequestPointWalkVillageAnswersForPlayerMessage m = (RequestPointWalkVillageAnswersForPlayerMessage) msg;
            int villageId = resolveVillageId(connection, m.getVillageID());
            connection.sendTCP(new RequestPointWalkVillageAnswersForPlayerResponseMessage(
                villageId,
                new ArrayList<>(),
                new ArrayList<>()
            ));
            return;
        }

        if (msg instanceof RequestPointWalkVillageHighscoreMessage) {
            RequestPointWalkVillageHighscoreMessage m = (RequestPointWalkVillageHighscoreMessage) msg;
            int villageId = resolveVillageId(connection, m.getVillageID());
            connection.sendTCP(new RequestPointWalkVillageHighscoreResponseMessage(
                villageId,
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>()
            ));
            return;
        }

        if (msg instanceof RequestPlayerClothesMessage) {
            RequestPlayerClothesMessage m = (RequestPlayerClothesMessage) msg;
            connection.sendTCP(new RequestPlayerClothesResponseMessage(m.getPlayerID(), new ArrayList<>()));
            return;
        }

        if (msg instanceof RequestPlayerImagesMessage) {
            RequestPlayerImagesMessage m = (RequestPlayerImagesMessage) msg;
            connection.sendTCP(new RequestPlayerImagesResonseMessage(m.getPlayerID(), "", new ArrayList<>()));
            return;
        }

        if (msg instanceof RequestQuestsMessage) {
            RequestQuestsMessage m = (RequestQuestsMessage) msg;
            connection.sendTCP(new RequestQuestsResponseMessage(
                m.getPlayerID(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>()
            ));
            return;
        }

        if (msg instanceof RequestSLXCreditsMessage) {
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            int amount = user != null ? DATABASE.getSLXCredits(user.userId) : 0;
            connection.sendTCP(new RequestSLXCreditsResponseMessage(amount));
            return;
        }

        if (msg instanceof SpendSLXCreditsMessage) {
            SpendSLXCreditsMessage m = (SpendSLXCreditsMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                connection.sendTCP(new RequestSLXCreditsResponseMessage(0));
                return;
            }
            try {
                int amount = DATABASE.spendSLXCredits(user.userId, m.getAmount());
                refreshCachedUserRecord(user.userId);
                connection.sendTCP(new RequestSLXCreditsResponseMessage(amount));
            } catch (IOException e) {
                log("[Server] Failed to spend SLX credits: " + e.getMessage());
            }
            return;
        }

        if (msg instanceof BuyCreditsMessage) {
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                connection.sendTCP(new RequestSLXCreditsResponseMessage(0));
                return;
            }
            int amount = DATABASE.getSLXCredits(user.userId);
            connection.sendTCP(new RequestSLXCreditsResponseMessage(amount));
            log("[Server] Ignored direct BuyCreditsMessage from userId=" + user.userId);
            return;
        }

        if (msg instanceof RequestAreNewGuestbookEntriesAvailableMessage) {
            connection.sendTCP(new AreNewGuestbookEntriesAvailableResponseMessage(false));
            return;
        }

        if (msg instanceof RequestAvatarDataMessage) {
            RequestAvatarDataMessage m = (RequestAvatarDataMessage) msg;
            int playerId = resolveReadablePlayerId(connection, m.getPlayerID());
            NordDatabase.UserRecord user = DATABASE.findByUserId(playerId);
            String fallbackName = user != null ? user.username : ("Player " + playerId);
            int fallbackLevel = user != null ? user.level : 1;
            AvatarDetails details = DATABASE.loadAvatarDetails(playerId, fallbackName, fallbackLevel);
            connection.sendTCP(new RequestAvatarDataResponseMessage(details));
            return;
        }

        if (msg instanceof StoreAvatarDataMessage) {
            StoreAvatarDataMessage m = (StoreAvatarDataMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                return;
            }
            try {
                DATABASE.storeAvatarData(user.userId, safeBytes(m.getData()));
            } catch (IOException e) {
                log("[Server] Failed to store avatar data: " + e.getMessage());
            }
            int villageId = ACTIVE_VILLAGE_BY_CONNECTION.getOrDefault(connection.getID(), 0);
            if (villageId != 0) {
                broadcastToVillage(villageId, new StoreAvatarDataNotificationMessage(villageId, user.userId, safeBytes(m.getData())), null, true, false);
            }
            return;
        }

        if (msg instanceof StoreMountMessage) {
            StoreMountMessage m = (StoreMountMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user != null) {
                try {
                    DATABASE.storeMount(user.userId, m.getMountType(), m.getMountName());
                } catch (IOException e) {
                    log("[Server] Failed to store mount: " + e.getMessage());
                }
            }
            return;
        }

        if (msg instanceof StorePetMessage) {
            StorePetMessage m = (StorePetMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user != null) {
                try {
                    DATABASE.storePet(user.userId, m.getPetType(), m.getPetName());
                } catch (IOException e) {
                    log("[Server] Failed to store pet: " + e.getMessage());
                }
            }
            return;
        }

        if (msg instanceof RequestPlayerInformationMessage) {
            RequestPlayerInformationMessage m = (RequestPlayerInformationMessage) msg;
            int playerId = resolveReadablePlayerId(connection, m.getPlayerID());
            NordDatabase.UserRecord user = DATABASE.findByUserId(playerId);
            String username = user != null ? user.username : "Player " + playerId;
            int homeVillage = user != null ? user.villageId : 0;
            int level = user != null ? user.level : 1;
            Connection targetConnection = findConnectionByPlayerId(playerId);
            boolean isOnline = targetConnection != null;
            int visitingVillage = 0;
            if (targetConnection != null) {
                visitingVillage = ACTIVE_VILLAGE_BY_CONNECTION.getOrDefault(targetConnection.getID(), homeVillage);
            }
            connection.sendTCP(new RequestPlayerInformationResponseMessage(
                playerId,
                username,
                "",
                username,
                0L,
                0,
                "", "",
                0,
                (byte) 0,
                homeVillage,
                visitingVillage,
                isOnline,
                "", "",
                false,
                level,
                (byte) 0,
                false,
                false
            ));
            return;
        }

        if (msg instanceof CompletedLevelMessage) {
            CompletedLevelMessage m = (CompletedLevelMessage) msg;
            int playerId = resolvePlayerId(connection, m.getPlayerID());
            if (playerId == 0) {
                return;
            }
            try {
                DATABASE.updateUserLevel(playerId, m.getLevel());
                refreshCachedUserRecord(playerId);
            } catch (IOException e) {
                log("[Server] Failed to persist completed level: " + e.getMessage());
            }
            return;
        }

        if (msg instanceof StoreSpinMessage) {
            StoreSpinMessage m = (StoreSpinMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                return;
            }
            try {
                DATABASE.storeSpinState(
                    user.userId,
                    m.getLevel(),
                    m.getWins(),
                    m.isWon(),
                    m.isLit1(),
                    m.isLit2(),
                    m.isLit3(),
                    m.isLit4(),
                    m.isLit5(),
                    m.isLit6(),
                    m.isLit7(),
                    m.isLit8(),
                    m.getRes1(),
                    m.getRes2(),
                    m.getRes3(),
                    m.getRes4(),
                    m.getRes5(),
                    m.getRes6(),
                    m.getRes7(),
                    m.getRes8()
                );
                DATABASE.updateUserLevel(user.userId, m.getLevel());
                refreshCachedUserRecord(user.userId);
            } catch (IOException e) {
                log("[Server] Failed to persist spin/inventory state: " + e.getMessage());
            }
            return;
        }

        if (msg instanceof AlterAchievementMessage) {
            AlterAchievementMessage m = (AlterAchievementMessage) msg;
            int playerId = resolvePlayerId(connection, m.getPlayerID());
            if (playerId == 0) {
                return;
            }
            try {
                int value = DATABASE.applyAchievementDelta(
                    playerId,
                    m.getCategory(),
                    m.getType(),
                    m.getChangeAmount(),
                    m.getAlterMethod() == com.nordclientpc.ZB.ADD
                );
                connection.sendTCP(new AchievementNotificationMessage(
                    playerId,
                    m.getCategory(),
                    m.getType(),
                    toShortClamp(value)
                ));
                log("[Server] AlterAchievement playerId=" + playerId +
                    " category=" + m.getCategory() +
                    " type=" + m.getType() +
                    " add=" + (m.getAlterMethod() == com.nordclientpc.ZB.ADD) +
                    " change=" + m.getChangeAmount() +
                    " storedValue=" + value);
            } catch (IOException e) {
                log("[Server] Failed to persist achievement delta: " + e.getMessage());
            }
            return;
        }

        String unhandledType = msg.getClass().getName();
        int seen = UNHANDLED_MESSAGE_COUNTS.merge(unhandledType, 1, Integer::sum);
        if (seen <= 5) {
            log("[Server] Unhandled message (" + seen + "): " + unhandledType);
        }
    }

    private static synchronized void joinVillage(Connection connection, int villageId) {
        if (villageId <= 0) {
            return;
        }

        NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
        if (user == null) {
            return;
        }

        int oldVillageId = ACTIVE_VILLAGE_BY_CONNECTION.getOrDefault(connection.getID(), 0);
        if (oldVillageId == villageId) {
            sendVillageSnapshot(connection, villageId);
            return;
        }

        if (oldVillageId != 0) {
            removeFromVillageConnectionMap(oldVillageId, connection.getID());
            broadcastToVillage(oldVillageId, new LeaveVillageWithAvatarNotificationMessage(oldVillageId, user.userId), connection, false, false);
        }

        ACTIVE_VILLAGE_BY_CONNECTION.put(connection.getID(), villageId);
        VILLAGE_CONNECTIONS.computeIfAbsent(villageId, key -> new ConcurrentHashMap<>()).put(connection.getID(), connection);

        sendVillageSnapshot(connection, villageId);

        ArrayList<AvatarDetails> entering = new ArrayList<>();
        entering.add(DATABASE.loadAvatarDetails(user.userId, user.username, user.level));
        broadcastToVillage(villageId, new EnterVillageWithAvatarNotificationMessage(villageId, entering), connection, false, false);
    }

    private static synchronized void leaveVillage(Connection connection, boolean notifyOthers) {
        int connectionId = connection.getID();
        int villageId = ACTIVE_VILLAGE_BY_CONNECTION.getOrDefault(connectionId, 0);
        if (villageId == 0) {
            return;
        }
        ACTIVE_VILLAGE_BY_CONNECTION.remove(connectionId);
        removeFromVillageConnectionMap(villageId, connectionId);

        if (notifyOthers) {
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connectionId);
            if (user != null) {
                broadcastToVillage(villageId, new LeaveVillageWithAvatarNotificationMessage(villageId, user.userId), connection, false, false);
            }
        }
    }

    private static synchronized void leaveVillageAndReturnHome(Connection connection) {
        NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
        int homeVillageId = user != null ? user.villageId : 0;
        leaveVillage(connection, true);
        if (homeVillageId <= 0) {
            return;
        }
        if (ACTIVE_VILLAGE_BY_CONNECTION.getOrDefault(connection.getID(), 0) != 0) {
            return;
        }
        joinVillage(connection, homeVillageId);
    }

    private static void sendVillageSnapshot(Connection connection, int villageId) {
        ArrayList<AvatarDetails> avatars = new ArrayList<>();
        ArrayList<Integer> playerIds = new ArrayList<>();
        Map<Integer, Connection> villageConnections = VILLAGE_CONNECTIONS.get(villageId);
        if (villageConnections != null) {
            for (Integer connectionId : villageConnections.keySet()) {
                NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connectionId);
                if (user != null) {
                    avatars.add(DATABASE.loadAvatarDetails(user.userId, user.username, user.level));
                    playerIds.add(user.userId);
                }
            }
        }
        connection.sendTCP(new EnterVillageWithAvatarNotificationMessage(villageId, avatars));
        for (Integer playerId : playerIds) {
            AvatarPosition position = AVATAR_POSITIONS_BY_PLAYER.get(playerId);
            if (position == null || position.villageId != villageId) {
                continue;
            }
            connection.sendTCP(new MoveAvatarNotificationMessage(
                villageId,
                playerId,
                position.posX,
                position.posZ,
                position.rotation
            ));
        }
    }

    private static void broadcastToVillage(int villageId, Object payload, Connection excludeConnection, boolean includeSender, boolean sendUdp) {
        Map<Integer, Connection> targets = VILLAGE_CONNECTIONS.get(villageId);
        if (targets == null || targets.isEmpty()) {
            return;
        }
        for (Connection target : targets.values()) {
            if (target == null) {
                continue;
            }
            if (excludeConnection != null && target.getID() == excludeConnection.getID() && !includeSender) {
                continue;
            }
            if (sendUdp) {
                target.sendUDP(payload);
            } else {
                target.sendTCP(payload);
            }
        }
    }

    private static void handleDisconnect(Connection connection) {
        logoutConnection(connection);
        CONNECTIONS_BY_ID.remove(connection.getID());
    }

    private static void logoutConnection(Connection connection) {
        int villageId = ACTIVE_VILLAGE_BY_CONNECTION.getOrDefault(connection.getID(), 0);
        NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
        if (user != null && villageId != 0) {
            broadcastToVillage(villageId, new PlayerLoggedOutNotificationMessage(villageId, user.userId), connection, false, false);
        }
        leaveVillage(connection, true);
        if (user != null) {
            AVATAR_POSITIONS_BY_PLAYER.remove(user.userId);
        }
        USERS_BY_CONNECTION.remove(connection.getID());
    }

    private static void removeFromVillageConnectionMap(int villageId, int connectionId) {
        Map<Integer, Connection> villageConnections = VILLAGE_CONNECTIONS.get(villageId);
        if (villageConnections == null) {
            return;
        }
        villageConnections.remove(connectionId);
        if (villageConnections.isEmpty()) {
            VILLAGE_CONNECTIONS.remove(villageId);
        }
    }

    private static boolean isConnectionInVillage(Connection connection, int villageId) {
        return ACTIVE_VILLAGE_BY_CONNECTION.getOrDefault(connection.getID(), 0) == villageId;
    }

    private static void ensureVillageSubscription(Connection connection, int villageId, String reason) {
        if (villageId <= 0 || connection == null) {
            return;
        }
        if (USERS_BY_CONNECTION.get(connection.getID()) == null) {
            return;
        }
        if (isConnectionInVillage(connection, villageId)) {
            return;
        }
        joinVillage(connection, villageId);
    }

    private static Connection findConnectionByPlayerId(int playerId) {
        for (Map.Entry<Integer, NordDatabase.UserRecord> entry : USERS_BY_CONNECTION.entrySet()) {
            NordDatabase.UserRecord user = entry.getValue();
            if (user != null && user.userId == playerId) {
                return CONNECTIONS_BY_ID.get(entry.getKey());
            }
        }
        return null;
    }

    private static int resolvePlayerId(Connection connection, int requestedPlayerId) {
        NordDatabase.UserRecord currentUser = USERS_BY_CONNECTION.get(connection.getID());
        if (currentUser == null) {
            return 0;
        }
        if (requestedPlayerId == 0 || requestedPlayerId == currentUser.userId) {
            return currentUser.userId;
        }
        log("[Server] Rejected cross-user playerId access from userId=" + currentUser.userId + " requestedPlayerId=" + requestedPlayerId);
        return 0;
    }

    private static int resolveReadablePlayerId(Connection connection, int requestedPlayerId) {
        NordDatabase.UserRecord currentUser = USERS_BY_CONNECTION.get(connection.getID());
        if (currentUser == null) {
            return 0;
        }
        if (requestedPlayerId != 0) {
            return requestedPlayerId;
        }
        return currentUser.userId;
    }

    private static int resolveVillageId(Connection connection, int requestedVillageId) {
        if (requestedVillageId != 0) {
            return requestedVillageId;
        }
        int currentVillage = ACTIVE_VILLAGE_BY_CONNECTION.getOrDefault(connection.getID(), 0);
        if (currentVillage != 0) {
            return currentVillage;
        }
        NordDatabase.UserRecord currentUser = USERS_BY_CONNECTION.get(connection.getID());
        return currentUser != null ? currentUser.villageId : 0;
    }

    private static int resolveLiveVillageId(Connection connection, int requestedVillageId) {
        NordDatabase.UserRecord currentUser = USERS_BY_CONNECTION.get(connection.getID());
        if (currentUser == null) {
            return 0;
        }
        int currentVillage = ACTIVE_VILLAGE_BY_CONNECTION.getOrDefault(connection.getID(), 0);
        if (currentVillage != 0) {
            return currentVillage;
        }
        if (requestedVillageId != 0 && requestedVillageId != currentUser.villageId) {
            log("[Server] Rejected cross-village write access from userId=" + currentUser.userId +
                " requestedVillageId=" + requestedVillageId + " homeVillageId=" + currentUser.villageId);
            return 0;
        }
        return currentUser.villageId;
    }

    private static PlayerDirectoryData buildPlayerDirectoryData(boolean onlyVillage, int villageIdFilter) {
        PlayerDirectoryData data = new PlayerDirectoryData();
        for (NordDatabase.UserRecord user : USERS_BY_CONNECTION.values()) {
            if (user == null) {
                continue;
            }
            if (onlyVillage && user.villageId != villageIdFilter) {
                continue;
            }
            data.names.add(user.username);
            data.villageIDs.add(user.villageId);
            data.userIDs.add(user.userId);
            data.ages.add(0);
            data.sexes.add((byte) 0);
            data.countries.add("");
            data.cities.add("");
            data.scores.add(0);
            data.onlines.add(true);
            data.photoURLs.add("");
            data.villageNames.add(resolveVillageName(user.villageId, user.villageName));
            data.visitors.add(villageVisitorCount(user.villageId));
            data.themes.add((byte) 0);
            data.levels.add(user.level);
        }
        return data;
    }

    private static int villageVisitorCount(int villageId) {
        Map<Integer, Connection> byVillage = VILLAGE_CONNECTIONS.get(villageId);
        return byVillage == null ? 0 : byVillage.size();
    }

    private static String resolveVillageName(int villageId, String fallbackVillageName) {
        NordDatabase.UserRecord owner = DATABASE.findByVillageId(villageId);
        if (owner != null && !safe(owner.villageName).isEmpty()) {
            return owner.villageName;
        }
        if (!safe(fallbackVillageName).isEmpty()) {
            return fallbackVillageName;
        }
        return "Village " + villageId;
    }

    private static Building findBuilding(int villageId, short buildingId) {
        ArrayList<Building> buildings = DATABASE.loadBuildings(villageId);
        for (Building building : buildings) {
            if (building != null && building.getBuildingID() == buildingId) {
                return building;
            }
        }
        return null;
    }

    private static void applyBuildingStateDelta(int villageId,
                                                short buildingId,
                                                byte[] parameters,
                                                Boolean consumed,
                                                Boolean placedOnMap,
                                                Boolean ready,
                                                boolean writeDelayStart,
                                                long delayEndOffset,
                                                Long ingredients) {
        if (villageId == 0) {
            return;
        }
        Building existing = findBuilding(villageId, buildingId);
        byte[] nextParameters = parameters != null ? safeBytes(parameters) : (existing != null ? safeBytes(existing.getParameters()) : new byte[0]);
        boolean nextConsumed = consumed != null ? consumed : (existing != null && existing.isConsumed());
        boolean nextPlacedOnMap = placedOnMap != null ? placedOnMap : (existing == null || existing.isPlacedOnMap());
        byte nextTileX = existing != null ? existing.getTileX() : 0;
        byte nextTileZ = existing != null ? existing.getTileZ() : 0;
        byte nextRotation = existing != null ? existing.getRotation() : 0;
        boolean nextReady = ready != null ? ready : (existing != null && existing.isReady());
        long now = System.currentTimeMillis();
        long previousDelayStart = existing != null ? existing.getDelayStart() : 0L;
        long previousDelayEnd = existing != null ? existing.getDelayEnd() : 0L;
        long previousDuration = previousDelayEnd > previousDelayStart ? (previousDelayEnd - previousDelayStart) : 0L;
        boolean restartDelayWindow = writeDelayStart;
        long nextDelayStart = restartDelayWindow ? now : previousDelayStart;
        long nextDelayEnd;
        if (delayEndOffset > 0) {
            nextDelayEnd = now + delayEndOffset;
        } else if (delayEndOffset == 0 && restartDelayWindow && previousDuration > 0) {
            // Some crop harvests restart with offset 0; preserve their configured growth duration.
            nextDelayEnd = now + previousDuration;
        } else if (delayEndOffset < 0 && restartDelayWindow && previousDuration > 0) {
            // Client often sends -1 on harvest; preserve configured production duration and restart timer now.
            nextDelayEnd = now + previousDuration;
        } else {
            nextDelayEnd = previousDelayEnd;
        }
        if (restartDelayWindow && nextDelayEnd <= nextDelayStart) {
            long fallbackDuration = previousDuration > 0 ? previousDuration : 30000L;
            nextDelayEnd = nextDelayStart + fallbackDuration;
        }
        if (nextDelayEnd > now) {
            // Server owns readiness while a cooldown is active.
            nextReady = false;
        }
        long nextIngredients = ingredients != null ? ingredients : (existing != null ? existing.getIngredients() : 0L);

        try {
            DATABASE.applyBuildingUpdate(
                villageId,
                buildingId,
                nextParameters,
                nextConsumed,
                nextPlacedOnMap,
                nextTileX,
                nextTileZ,
                nextRotation,
                nextReady,
                nextDelayStart,
                nextDelayEnd,
                nextIngredients
            );
        } catch (IOException e) {
            log("[Server] Failed to apply building delta for " + buildingId + ": " + e.getMessage());
        }

        broadcastToVillage(
            villageId,
            new UpdateBuildingNotificationMessage(
                villageId,
                buildingId,
                nextParameters,
                nextConsumed,
                nextPlacedOnMap,
                nextTileX,
                nextTileZ,
                nextRotation,
                nextReady,
                nextDelayStart,
                nextDelayEnd,
                nextIngredients
            ),
            null,
            true,
            false
        );
    }

    private static class PlayerDirectoryData {
        final ArrayList<String> names = new ArrayList<>();
        final ArrayList<Integer> villageIDs = new ArrayList<>();
        final ArrayList<Integer> userIDs = new ArrayList<>();
        final ArrayList<Integer> ages = new ArrayList<>();
        final ArrayList<Byte> sexes = new ArrayList<>();
        final ArrayList<String> countries = new ArrayList<>();
        final ArrayList<String> cities = new ArrayList<>();
        final ArrayList<Integer> scores = new ArrayList<>();
        final ArrayList<Boolean> onlines = new ArrayList<>();
        final ArrayList<String> photoURLs = new ArrayList<>();
        final ArrayList<String> villageNames = new ArrayList<>();
        final ArrayList<Integer> visitors = new ArrayList<>();
        final ArrayList<Byte> themes = new ArrayList<>();
        final ArrayList<Integer> levels = new ArrayList<>();
    }

    private static class AvatarPosition {
        final int villageId;
        final short posX;
        final short posZ;
        final short rotation;

        AvatarPosition(int villageId, short posX, short posZ, short rotation) {
            this.villageId = villageId;
            this.posX = posX;
            this.posZ = posZ;
            this.rotation = rotation;
        }
    }

    private static NordDatabase.UserRecord createOrGetSocialUser(String username) {
        try {
            NordDatabase.UserRecord user = DATABASE.findOrCreateSocialUser(username);
            if (user != null) {
                log("[Server] Social login user " + user.username + " (userId=" + user.userId + ")");
            }
            return user;
        } catch (IOException e) {
            log("[Server] Failed social login create/find for " + username + ": " + e.getMessage());
            return null;
        }
    }

    private static NordDatabase.CreateUserResult createUser(String username, String password) {
        try {
            NordDatabase.CreateUserResult result = DATABASE.createUser(username, password);
            if (result.success) {
                log("[Server] Created user " + result.user.username + " (userId=" + result.user.userId + ", villageId=" + result.user.villageId + ")");
            }
            return result;
        } catch (IOException e) {
            log("[Server] Failed to persist user " + username + ": " + e.getMessage());
            return new NordDatabase.CreateUserResult(false, false, null);
        }
    }

    private static void sendLoginFailure(Connection connection) {
        connection.sendTCP(new LoginResponseMessage(
            LOGIN_FAILED_RESPONSE_CODE,
            0,
            0,
            new ArrayList<Integer>(),
            new ArrayList<String>(),
            new byte[0],
            0,
            0,
            0,
            0,
            false,
            0
        ));
    }

    private static void refreshCachedUserRecord(int userId) {
        if (userId <= 0) {
            return;
        }
        NordDatabase.UserRecord fresh = DATABASE.findByUserId(userId);
        if (fresh == null) {
            return;
        }
        for (Map.Entry<Integer, NordDatabase.UserRecord> entry : USERS_BY_CONNECTION.entrySet()) {
            NordDatabase.UserRecord current = entry.getValue();
            if (current != null && current.userId == userId) {
                USERS_BY_CONNECTION.put(entry.getKey(), fresh);
            }
        }
    }

    private static void sendLoginSuccess(Connection connection, NordDatabase.UserRecord user) {
        ArrayList<Integer> villageIDs = new ArrayList<>();
        ArrayList<String> villageNames = new ArrayList<>();
        villageIDs.add(user.villageId);
        villageNames.add(user.villageName);

        int sessionId = (int) (System.currentTimeMillis() & 0x7fffffff);
        connection.sendTCP(new LoginResponseMessage(
            0,
            sessionId,
            user.userId,
            villageIDs,
            villageNames,
            new byte[0],
            user.level,
            0,
            0,
            0,
            true,
            0
        ));
    }

    private static void sendBlockedPlayersSnapshot(Connection connection, int playerId) {
        connection.sendTCP(new RequestBlockedPlayersResponseMessage(
            playerId,
            new ArrayList<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            new ArrayList<>()
        ));
    }

    private static void sendAchievementSnapshot(Connection connection, int playerId) {
        if (playerId <= 0) {
            return;
        }
        ArrayList<NordDatabase.AchievementRecord> achievements = DATABASE.loadAchievements(playerId);
        for (NordDatabase.AchievementRecord achievement : achievements) {
            connection.sendTCP(new AchievementNotificationMessage(
                playerId,
                achievement.category,
                achievement.type,
                toShortClamp(achievement.value)
            ));
        }
    }

    private static void sendIMListSnapshot(Connection connection, int playerId) {
        ArrayList<com.slx.nord.jgnpersistentobjectdetails.IMListPlayerData> list = new ArrayList<>();
        for (NordDatabase.UserRecord user : USERS_BY_CONNECTION.values()) {
            if (user == null) {
                continue;
            }
            list.add(new com.slx.nord.jgnpersistentobjectdetails.IMListPlayerData(
                user.userId,
                user.username,
                true,
                0,
                (byte) 0,
                user.villageId,
                villageVisitorCount(user.villageId),
                "",
                "",
                "",
                (byte) 0,
                user.level
            ));
        }
        connection.sendTCP(new RequestIMListResponseMessage(playerId, list));
    }

    private static String safe(String s) {
        return s == null ? "" : s;
    }

    private static byte[] safeBytes(byte[] bytes) {
        return bytes == null ? new byte[0] : bytes;
    }

    private static short toShortClamp(int value) {
        if (value > Short.MAX_VALUE) {
            return Short.MAX_VALUE;
        }
        if (value < Short.MIN_VALUE) {
            return Short.MIN_VALUE;
        }
        return (short) value;
    }

    private static int resolveDefaultBuildingCost() {
        String fromProperty = System.getProperty("nord.default.building.cost");
        if (!safe(fromProperty).isEmpty()) {
            try {
                return Math.max(0, Integer.parseInt(fromProperty.trim()));
            } catch (NumberFormatException ignored) {
                // fall through
            }
        }
        String fromEnv = System.getenv("NORD_DEFAULT_BUILDING_COST");
        if (!safe(fromEnv).isEmpty()) {
            try {
                return Math.max(0, Integer.parseInt(fromEnv.trim()));
            } catch (NumberFormatException ignored) {
                // fall through
            }
        }
        return 1;
    }

    private static Map<Short, Integer> loadBuildingCostOverrides() {
        Map<Short, Integer> result = new HashMap<>();
        String fromProperty = System.getProperty("nord.building.costs.path");
        String fromEnv = System.getenv("NORD_BUILDING_COSTS_PATH");
        String pathValue = !safe(fromProperty).isEmpty() ? fromProperty : (!safe(fromEnv).isEmpty() ? fromEnv : DEFAULT_BUILDING_COSTS_PATH);
        Path path = Paths.get(pathValue);
        if (!Files.exists(path)) {
            return result;
        }
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(path)) {
            properties.load(in);
        } catch (IOException e) {
            log("[Server] Failed to load building cost overrides from " + path + ": " + e.getMessage());
            return result;
        }
        for (String key : properties.stringPropertyNames()) {
            String value = properties.getProperty(key);
            try {
                short buildingType = Short.parseShort(key.trim());
                int cost = Math.max(0, Integer.parseInt(safe(value).trim()));
                result.put(buildingType, cost);
            } catch (NumberFormatException ignored) {
                log("[Server] Ignoring invalid building cost override entry: " + key + "=" + value);
            }
        }
        return result;
    }

    private static int resolveClientDisplayedBuildingCost(Building building) {
        if (building == null) {
            return 0;
        }
        try {
            com.nordclientpc.acI definition = com.nordclientpc.acI.b(building.getBuildingType());
            if (definition != null) {
                int uiCost = definition.k();
                if (uiCost > 0) {
                    return uiCost;
                }
            }
        } catch (Throwable ignored) {
            // Fall through to override/default.
        }
        Integer override = SERVER_BUILDING_COST_OVERRIDES.get(building.getBuildingType());
        if (override != null) {
            return Math.max(0, override);
        }
        return DEFAULT_SERVER_BUILDING_COST;
    }

    private static int resolveAddBuildingCreditsDelta(AddBuildingMessage message) {
        if (message == null) {
            return 0;
        }
        if (message.getExtraCredits() != 0) {
            return message.getExtraCredits() < 0 ? message.getExtraCredits() : 0;
        }
        String maybeDelta = safe(message.getExtraMessage()).trim();
        if (maybeDelta.isEmpty()) {
            return 0;
        }
        if (!maybeDelta.matches("^[+-]?\\d+$")) {
            return 0;
        }
        try {
            int delta = Integer.parseInt(maybeDelta);
            return delta < 0 ? delta : 0;
        } catch (NumberFormatException ignored) {
            return 0;
        }
    }

    private static String resolveDatabasePath() {
        String fromProperty = System.getProperty("nord.db.path");
        if (!safe(fromProperty).isEmpty()) {
            return fromProperty;
        }
        String fromEnv = System.getenv("NORD_DB_PATH");
        if (!safe(fromEnv).isEmpty()) {
            return fromEnv;
        }
        return DEFAULT_DB_PATH;
    }

    private static void log(String message) {
        System.out.println(message);
        System.out.flush();
    }
}
