import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.esotericsoftware.minlog.Log;
import com.slx.nord.jgnmessages.JGNMessages;
import com.slx.nord.jgnmessages.TreasureType;
import com.slx.nord.jgnmessages.bytearraymessages.AddServerSessionParametersMessage;
import com.slx.nord.jgnmessages.bytearraymessages.BindSessionToClientMessage;
import com.slx.nord.jgnmessages.bytearraymessages.BlockPlayerMessage;
import com.slx.nord.jgnmessages.bytearraymessages.BuildingNotAddedNotEnoughCreditsMessage;
import com.slx.nord.jgnmessages.bytearraymessages.BuildingAddedMessage;
import com.slx.nord.jgnmessages.bytearraymessages.BuildingsChecksumResponseMessage;
import com.slx.nord.jgnmessages.bytearraymessages.CouldNotBanUserMessage;
import com.slx.nord.jgnmessages.bytearraymessages.GetBuildingsChecksumMessage;
import com.slx.nord.jgnmessages.bytearraymessages.LikedOnFacebookEventMessage;
import com.slx.nord.jgnmessages.bytearraymessages.LoggedOutByNewLoginMessage;
import com.slx.nord.jgnmessages.bytearraymessages.LoggingInWithOldClientMessage;
import com.slx.nord.jgnmessages.bytearraymessages.PickedUpTreasureMessage;
import com.slx.nord.jgnmessages.bytearraymessages.PlayerBlacklistedMessage;
import com.slx.nord.jgnmessages.bytearraymessages.PlayerHasYouBlockedMessage;
import com.slx.nord.jgnmessages.bytearraymessages.RequestTwitterTokensMessage;
import com.slx.nord.jgnmessages.bytearraymessages.SponsorpayNotificationMessage;
import com.slx.nord.jgnmessages.bytearraymessages.StoreTwitterTokensMessage;
import com.slx.nord.jgnmessages.bytearraymessages.SuperRewardsNotificationMessage;
import com.slx.nord.jgnmessages.bytearraymessages.TeamBonusMessage;
import com.slx.nord.jgnmessages.bytearraymessages.TwitterTokensResponseMessage;
import com.slx.nord.jgnmessages.bytearraymessages.UnblockPlayerMessage;
import com.slx.nord.jgnmessages.clientinbound.ReceiveEventMessage;
import com.slx.nord.jgnmessages.clientinbound.notifications.*;
import com.slx.nord.jgnmessages.clientinbound.responses.*;
import com.slx.nord.jgnmessages.clientoutbound.*;
import com.slx.nord.jgnpersistentobjectdetails.AvatarDetails;
import com.slx.nord.jgnpersistentobjectdetails.Building;
import com.slx.nord.jgnpersistentobjectdetails.Tree;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.imageio.ImageIO;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;

public class NordServer {
    private static final int TCP_PORT = 41210;
    private static final int UDP_PORT = 41211;

    private static final int LOGIN_FAILED_RESPONSE_CODE = 1;
    private static final int CREATE_ACCOUNT_USERNAME_TAKEN = 1;
    private static final int CREATE_ACCOUNT_INVALID_USERNAME = 2;
    private static final short SHARE_TYPE_RESOURCE = 3;
    private static final String DEFAULT_DB_PATH = "nord-data.sqlite";
    private static final String DEFAULT_RANDOMSEED_PATH = "randomseed.dat";
    private static final String RANDOMSEED_PASSWORD = "987123jklasduiorewmn1234";
    private static final byte[] RANDOMSEED_SALT = new byte[] {(byte) 0x8e, 0x12, 0x39, (byte) 0x9c, 0x07, 0x72, 0x6f, 0x5a};
    private static final int RANDOMSEED_ITERATIONS = 10;
    private static final int DEFAULT_LEVEL_UP_REWARD_BASE_CREDITS = 75;
    private static final int DEFAULT_LEVEL_UP_REWARD_STEP_CREDITS = 5;
    private static final int DEFAULT_SMS_BUY_CREDITS_AMOUNT = 1000;
    private static final int DEFAULT_PLAYER_IMAGES_LIMIT = 64;
    private static final int INVALID_VILLAGE_ID_SENTINEL = Integer.MAX_VALUE;
    private static final short ACHIEVEMENT_ROLE_CATEGORY = 6;
    private static final short ACHIEVEMENT_VIP_TYPE = 0;
    private static final short ACHIEVEMENT_ADMIN_TYPE = 1;
    private static final short ACHIEVEMENT_MODERATOR_TYPE = 2;
    private static final String CHAT_COMMAND_SET_COINS = "setcoins";
    private static final String CHAT_COMMAND_SET_LEVEL = "setlevel";
    private static final String CHAT_COMMAND_PERMISSION_EVERYONE = "everyone";
    private static final String CHAT_COMMAND_PERMISSION_VIP = "vip";
    private static final String CHAT_COMMAND_PERMISSION_MOD = "mod";
    private static final Set<String> SUPPORTED_CHAT_COMMANDS = Collections.unmodifiableSet(
        new LinkedHashSet<>(Arrays.asList(CHAT_COMMAND_SET_COINS, CHAT_COMMAND_SET_LEVEL))
    );
    private static final int COULD_NOT_BAN_REASON_MODERATOR_OR_ADMIN = 0;
    private static final int DEFAULT_TEAM_BONUS_ID = 1;
    private static final int MAX_GENERIC_BYTEARRAY_BYTES = 65536;
    private static final int SERVER_MODAL_MESSAGE_PLAYER_ID = Integer.MIN_VALUE;
    private static final String SERVER_MODAL_MESSAGE_COMMAND_DEFAULT = "update";
    private static final int ADMIN_POPUP_QUEUE_BATCH_LIMIT = 32;
    private static final long ADMIN_POPUP_QUEUE_POLL_INTERVAL_MS = 500L;
    private static final long RESOURCE_RESPAWN_SCAN_INTERVAL_MS = 1500L;
    private static final long DEFAULT_WILD_RESOURCE_RESPAWN_DELAY_MS = TimeUnit.MINUTES.toMillis(30L);
    private static final short DEFAULT_AVATAR_POS_X = 0;
    private static final short DEFAULT_AVATAR_POS_Z = 0;
    private static final short DEFAULT_AVATAR_ROTATION = 0;
    private static final long[] INITIAL_AVATAR_MOVE_RETRY_DELAYS_MS = new long[] {350L, 1000L, 2200L};
    private static final Pattern BUILDING_TAG_PATTERN = Pattern.compile("<BUILDING\\b[^>]*>", Pattern.CASE_INSENSITIVE);
    private static final Pattern HORSE_TAG_PATTERN = Pattern.compile("<HORSE\\b[^>]*>", Pattern.CASE_INSENSITIVE);
    private static final Pattern PET_TAG_PATTERN = Pattern.compile("<PET\\b[^>]*>", Pattern.CASE_INSENSITIVE);
    private static final Pattern RECIPE_TAG_PATTERN = Pattern.compile("<RECIPE\\b[^>]*>", Pattern.CASE_INSENSITIVE);
    private static final Pattern ATTRIBUTE_PATTERN = Pattern.compile("(\\w+)\\s*=\\s*\"([^\"]*)\"");
    private static final Pattern CREDITS_WITH_K_PATTERN = Pattern.compile("(\\d{1,3})\\s*[kK]");
    private static final Pattern COMPANION_PURCHASE_LOG_PATTERN =
        Pattern.compile("companion_purchase_(mount|pet)_(\\d+)", Pattern.CASE_INSENSITIVE);
    private static final Set<String> ALLOWED_INBOUND_GENERIC_PAYLOAD_CLASSES = Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(
        AddServerSessionParametersMessage.class.getName(),
        BindSessionToClientMessage.class.getName(),
        BlockPlayerMessage.class.getName(),
        GetBuildingsChecksumMessage.class.getName(),
        RequestTwitterTokensMessage.class.getName(),
        StoreTwitterTokensMessage.class.getName(),
        UnblockPlayerMessage.class.getName()
    )));

    private static final int HEIGHTMAP_PRESET_MAP_SIZE = 129;
    private static final byte[] DEFAULT_HEIGHTMAP = buildDefaultHeightMap();

    private static final Map<Integer, Connection> CONNECTIONS_BY_ID = new ConcurrentHashMap<>();
    private static final Map<Integer, byte[]> HEIGHTMAP_PRESET_BYTES_BY_ID = new ConcurrentHashMap<>();
    private static final Map<Integer, NordDatabase.UserRecord> USERS_BY_CONNECTION = new ConcurrentHashMap<>();
    private static final Map<Integer, Integer> SERVER_SESSION_ID_BY_CONNECTION = new ConcurrentHashMap<>();
    private static final Map<Integer, Integer> PLAYER_ID_BY_SERVER_SESSION_ID = new ConcurrentHashMap<>();
    private static final Map<Integer, Integer> ACTIVE_VILLAGE_BY_CONNECTION = new ConcurrentHashMap<>();
    private static final Map<Integer, Map<Integer, Connection>> VILLAGE_CONNECTIONS = new ConcurrentHashMap<>();
    private static final Map<Integer, Long> LAST_RESOURCE_RESPAWN_SCAN_BY_VILLAGE = new ConcurrentHashMap<>();
    private static final Map<Integer, Integer> LAST_BUILDINGS_SYNC_VILLAGE_BY_CONNECTION = new ConcurrentHashMap<>();
    private static final Map<Integer, AvatarPosition> AVATAR_POSITIONS_BY_PLAYER = new ConcurrentHashMap<>();
    private static final Map<String, Integer> UNHANDLED_MESSAGE_COUNTS = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService AVATAR_POSITION_REPLAY_EXECUTOR = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "nord-avatar-position-replay");
            thread.setDaemon(true);
            return thread;
        }
    });
    private static final ScheduledExecutorService ADMIN_POPUP_QUEUE_EXECUTOR = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "nord-admin-popup-queue");
            thread.setDaemon(true);
            return thread;
        }
    });
    private static volatile Map<Short, Integer> BUILDING_BASE_PRICE_BY_TYPE = Collections.emptyMap();
    private static volatile Map<Short, TreasureType> BUILDING_TREASURE_TYPE_BY_TYPE = Collections.emptyMap();
    private static volatile Map<Short, Integer> BUILDING_REQUIRED_INGREDIENT_COUNT_BY_TYPE = Collections.emptyMap();
    private static volatile Set<Short> BUILDING_GIFT_TYPE_IDS = Collections.emptySet();
    private static volatile Set<Short> RESOURCE_HARVEST_BUILDING_TYPE_IDS = Collections.emptySet();
    private static volatile Map<Short, CompanionCatalogEntry> MOUNT_CATALOG_BY_TYPE = Collections.emptyMap();
    private static volatile Map<Short, CompanionCatalogEntry> PET_CATALOG_BY_TYPE = Collections.emptyMap();
    private static volatile int LEVEL_UP_REWARD_BASE_CREDITS = DEFAULT_LEVEL_UP_REWARD_BASE_CREDITS;
    private static volatile int LEVEL_UP_REWARD_STEP_CREDITS = DEFAULT_LEVEL_UP_REWARD_STEP_CREDITS;
    private static volatile Map<String, Integer> BUY_CREDITS_AMOUNT_BY_CODE = Collections.emptyMap();
    private static volatile long WILD_RESOURCE_RESPAWN_DELAY_MS = DEFAULT_WILD_RESOURCE_RESPAWN_DELAY_MS;
    private static volatile boolean ENABLE_BUILDINGS_CHECKSUM_RESPONSES = true;
    private static volatile boolean REQUIRE_PROVIDER_CALLBACK_BUY_CREDITS = false;

    private static NordDatabase DATABASE;
    private static boolean UDP_ENABLED = true;

    private enum BuyCreditsSource {
        UNKNOWN,
        SPONSORPAY,
        SUPERREWARDS,
        FACEBOOK,
        TEAM_BONUS
    }

    private enum ChatCommandAccessLevel {
        EVERYONE,
        VIP,
        MOD
    }

    private static final class BuildingEconomyCatalog {
        final Map<Short, Integer> pricesByType;
        final Map<Short, TreasureType> treasureTypesByType;
        final Map<Short, Integer> requiredIngredientCountsByType;
        final Set<Short> giftBuildingTypes;
        final Set<Short> resourceHarvestBuildingTypes;

        private BuildingEconomyCatalog(Map<Short, Integer> pricesByType,
                                       Map<Short, TreasureType> treasureTypesByType,
                                       Map<Short, Integer> requiredIngredientCountsByType,
                                       Set<Short> giftBuildingTypes,
                                       Set<Short> resourceHarvestBuildingTypes) {
            this.pricesByType = pricesByType;
            this.treasureTypesByType = treasureTypesByType;
            this.requiredIngredientCountsByType = requiredIngredientCountsByType;
            this.giftBuildingTypes = giftBuildingTypes;
            this.resourceHarvestBuildingTypes = resourceHarvestBuildingTypes;
        }
    }

    private static final class CompanionCatalogEntry {
        final String kind;
        final short type;
        final int price;
        final int requiredLevel;
        final int canBuyFromMonthDay;
        final int canBuyToMonthDay;

        private CompanionCatalogEntry(String kind,
                                      short type,
                                      int price,
                                      int requiredLevel,
                                      int canBuyFromMonthDay,
                                      int canBuyToMonthDay) {
            this.kind = kind;
            this.type = type;
            this.price = Math.max(0, price);
            this.requiredLevel = Math.max(0, requiredLevel);
            this.canBuyFromMonthDay = Math.max(0, canBuyFromMonthDay);
            this.canBuyToMonthDay = Math.max(0, canBuyToMonthDay);
        }
    }

    private static final class CompanionCatalog {
        final Map<Short, CompanionCatalogEntry> mountsByType;
        final Map<Short, CompanionCatalogEntry> petsByType;

        private CompanionCatalog(Map<Short, CompanionCatalogEntry> mountsByType,
                                 Map<Short, CompanionCatalogEntry> petsByType) {
            this.mountsByType = mountsByType;
            this.petsByType = petsByType;
        }
    }

    private static final class CompanionPurchaseRequest {
        final String kind;
        final short companionType;

        private CompanionPurchaseRequest(String kind, short companionType) {
            this.kind = kind;
            this.companionType = companionType;
        }
    }

    private static final class TreasurePickupReward {
        final TreasureType treasureType;
        final int coinsInTreasure;

        private TreasurePickupReward(TreasureType treasureType, int coinsInTreasure) {
            this.treasureType = treasureType;
            this.coinsInTreasure = coinsInTreasure;
        }
    }

    private static final class WhitelistObjectInputStream extends ObjectInputStream {
        private final Set<String> allowedClassNames;

        private WhitelistObjectInputStream(ByteArrayInputStream in, Set<String> allowedClassNames) throws IOException {
            super(in);
            this.allowedClassNames = allowedClassNames;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass descriptor) throws IOException, ClassNotFoundException {
            String className = descriptor == null ? null : descriptor.getName();
            if (!isAllowedSerializedClassName(className, allowedClassNames)) {
                throw new InvalidClassException("Disallowed serialized class", className);
            }
            return super.resolveClass(descriptor);
        }
    }

    private static byte[] buildDefaultHeightMap() {
        int size = HEIGHTMAP_PRESET_MAP_SIZE;
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

    private static byte[] resolveHeightMapFallbackForVillage(int villageId) {
        if (villageId <= 0 || DATABASE == null) {
            return DEFAULT_HEIGHTMAP;
        }
        NordDatabase.UserRecord owner = DATABASE.findByVillageId(villageId);
        if (owner == null || owner.heightmapPresetId <= 0) {
            return DEFAULT_HEIGHTMAP;
        }
        byte[] cached = HEIGHTMAP_PRESET_BYTES_BY_ID.get(owner.heightmapPresetId);
        if (cached != null) {
            return cached;
        }
        byte[] loaded = loadHeightMapPresetBytes(owner.heightmapPresetId);
        if (loaded == null || loaded.length != DEFAULT_HEIGHTMAP.length) {
            return DEFAULT_HEIGHTMAP;
        }
        byte[] existing = HEIGHTMAP_PRESET_BYTES_BY_ID.putIfAbsent(owner.heightmapPresetId, loaded);
        return existing != null ? existing : loaded;
    }

    private static byte[] loadHeightMapPresetBytes(int presetId) {
        if (presetId <= 0) {
            return null;
        }
        String resourcePath = "heightmaps/" + presetId + ".png";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader == null) {
            loader = NordServer.class.getClassLoader();
        }
        try (InputStream input = loader != null
            ? loader.getResourceAsStream(resourcePath)
            : ClassLoader.getSystemResourceAsStream(resourcePath)) {
            if (input == null) {
                log("[Server] Missing Landscape preset resource " + resourcePath + ", using default heightmap");
                return null;
            }
            BufferedImage image = ImageIO.read(input);
            if (image == null) {
                log("[Server] Failed to decode Landscape preset " + resourcePath + ", using default heightmap");
                return null;
            }
            if (image.getWidth() != HEIGHTMAP_PRESET_MAP_SIZE || image.getHeight() != HEIGHTMAP_PRESET_MAP_SIZE) {
                log("[Server] Unexpected Landscape preset dimensions for " + resourcePath +
                    ": " + image.getWidth() + "x" + image.getHeight() +
                    " (expected " + HEIGHTMAP_PRESET_MAP_SIZE + "x" + HEIGHTMAP_PRESET_MAP_SIZE + ")");
                return null;
            }
            int[] rgb = image.getRGB(0, 0, HEIGHTMAP_PRESET_MAP_SIZE, HEIGHTMAP_PRESET_MAP_SIZE, null, 0, HEIGHTMAP_PRESET_MAP_SIZE);
            byte[] converted = new byte[rgb.length];
            for (int i = 0; i < rgb.length; i++) {
                int blue = rgb[i] & 0xFF;
                int value = 7 - ((255 - blue) / 4);
                converted[i] = (byte) value;
            }
            return converted;
        } catch (IOException e) {
            log("[Server] Failed to load Landscape preset " + resourcePath + ": " + e.getMessage());
            return null;
        }
    }

    public static void main(String[] args) throws IOException {
        String databasePath = resolveDatabasePath();
        DATABASE = new NordDatabase(databasePath);
        int[] levelRewardConfig = resolveLevelUpRewardConfig();
        LEVEL_UP_REWARD_BASE_CREDITS = levelRewardConfig[0];
        LEVEL_UP_REWARD_STEP_CREDITS = levelRewardConfig[1];
        BUY_CREDITS_AMOUNT_BY_CODE = resolveBuyCreditsCodeMap();
        WILD_RESOURCE_RESPAWN_DELAY_MS = resolveWildResourceRespawnDelayMs();
        ENABLE_BUILDINGS_CHECKSUM_RESPONSES = resolveBuildingsChecksumResponsesEnabled();
        REQUIRE_PROVIDER_CALLBACK_BUY_CREDITS = resolveRequireProviderCallbackBuyCredits();
        Path randomseedPath = resolveRandomseedPath();
        BuildingEconomyCatalog economyCatalog = loadBuildingEconomyCatalog(randomseedPath);
        CompanionCatalog companionCatalog = loadCompanionCatalog(randomseedPath);
        Map<Short, Integer> buildingPrices = Collections.unmodifiableMap(new HashMap<>(economyCatalog.pricesByType));
        Map<Short, TreasureType> buildingTreasureTypes =
            Collections.unmodifiableMap(new HashMap<>(economyCatalog.treasureTypesByType));
        Map<Short, Integer> buildingRequiredIngredientCounts =
            Collections.unmodifiableMap(new HashMap<>(economyCatalog.requiredIngredientCountsByType));
        Set<Short> giftBuildingTypes = Collections.unmodifiableSet(new LinkedHashSet<>(economyCatalog.giftBuildingTypes));
        Set<Short> resourceHarvestBuildingTypes =
            Collections.unmodifiableSet(new LinkedHashSet<>(economyCatalog.resourceHarvestBuildingTypes));
        Map<Short, CompanionCatalogEntry> mountCatalogByType =
            Collections.unmodifiableMap(new HashMap<>(companionCatalog.mountsByType));
        Map<Short, CompanionCatalogEntry> petCatalogByType =
            Collections.unmodifiableMap(new HashMap<>(companionCatalog.petsByType));
        BUILDING_BASE_PRICE_BY_TYPE = Collections.unmodifiableMap(buildingPrices);
        BUILDING_TREASURE_TYPE_BY_TYPE = buildingTreasureTypes;
        BUILDING_REQUIRED_INGREDIENT_COUNT_BY_TYPE = buildingRequiredIngredientCounts;
        BUILDING_GIFT_TYPE_IDS = giftBuildingTypes;
        RESOURCE_HARVEST_BUILDING_TYPE_IDS = resourceHarvestBuildingTypes;
        MOUNT_CATALOG_BY_TYPE = mountCatalogByType;
        PET_CATALOG_BY_TYPE = petCatalogByType;
        DATABASE.setBuildingBasePrices(buildingPrices);
        log("[Server] Loaded ingredient requirements for " + BUILDING_REQUIRED_INGREDIENT_COUNT_BY_TYPE.size() + " building type(s)");
        log("[Server] Loaded wild resource building types: " + RESOURCE_HARVEST_BUILDING_TYPE_IDS.size() +
            " (fallback respawn delay=" + WILD_RESOURCE_RESPAWN_DELAY_MS + "ms)");
        try {
            int restoredWildResources = DATABASE.restoreStuckResourceCooldownRows(RESOURCE_HARVEST_BUILDING_TYPE_IDS);
            if (restoredWildResources > 0) {
                log("[Server] Restored " + restoredWildResources + " stuck wild resource cooldown row(s)");
            }
        } catch (IOException e) {
            log("[Server] Failed restoring stuck wild resource cooldown rows: " + e.getMessage());
        }

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

        if (buildingPrices.isEmpty()) {
            log("[Server] WARNING: no building prices loaded from " + randomseedPath.toAbsolutePath() + " (or fallback paths); purchase validation will be limited");
        } else {
            log("[Server] Loaded " + buildingPrices.size() + " building base prices");
        }
        if (!buildingTreasureTypes.isEmpty()) {
            log("[Server] Loaded " + buildingTreasureTypes.size() + " treasure-enabled building definitions");
        }
        if (!giftBuildingTypes.isEmpty()) {
            log("[Server] Loaded " + giftBuildingTypes.size() + " gift-enabled building definitions");
        }
        log("[Server] Loaded companion catalog entries: mounts=" + mountCatalogByType.size() +
            " pets=" + petCatalogByType.size());
        log("[Server] Level-up reward formula: base=" + LEVEL_UP_REWARD_BASE_CREDITS +
            " step=" + LEVEL_UP_REWARD_STEP_CREDITS +
            " (credits = base + completedLevel*step)");
        log("[Server] Buy-credits code mappings loaded: " + BUY_CREDITS_AMOUNT_BY_CODE.size());
        log("[Server] Buildings checksum responses enabled: " + ENABLE_BUILDINGS_CHECKSUM_RESPONSES);
        String requiredClientVersion = resolveRequiredClientVersion();
        if (!requiredClientVersion.isEmpty()) {
            log("[Server] Enforcing client version: " + requiredClientVersion);
        }

        if (enableUdp) {
            server.bind(TCP_PORT, UDP_PORT);
            log("Nord standalone server listening on TCP " + TCP_PORT + " / UDP " + UDP_PORT);
        } else {
            server.bind(TCP_PORT);
            log("Nord standalone server listening on TCP " + TCP_PORT + " (UDP disabled)");
        }
        log("Database path: " + databasePath + " (users=" + DATABASE.getUserCount() + ")");
        server.start();
        startAdminPopupQueueDispatcher();
    }

    private static void startAdminPopupQueueDispatcher() {
        ADMIN_POPUP_QUEUE_EXECUTOR.scheduleWithFixedDelay(() -> {
            try {
                dispatchPendingAdminPopups();
            } catch (Throwable t) {
                log("[Server] Admin popup dispatch loop failed: " + t.getMessage());
            }
        }, ADMIN_POPUP_QUEUE_POLL_INTERVAL_MS, ADMIN_POPUP_QUEUE_POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private static void dispatchPendingAdminPopups() {
        if (DATABASE == null) {
            return;
        }
        ArrayList<NordDatabase.AdminPopupQueueRecord> queuedPopups =
            DATABASE.loadPendingAdminPopupQueue(ADMIN_POPUP_QUEUE_BATCH_LIMIT);
        if (queuedPopups.isEmpty()) {
            return;
        }
        for (NordDatabase.AdminPopupQueueRecord popup : queuedPopups) {
            if (popup == null) {
                continue;
            }
            String senderCommand = safe(popup.senderName).trim();
            if (senderCommand.isEmpty()) {
                senderCommand = SERVER_MODAL_MESSAGE_COMMAND_DEFAULT;
            }
            String messageText = safe(popup.messageText).trim();
            try {
                if (!messageText.isEmpty()) {
                    broadcastToOnline(
                        new SendTextToPlayerNotificationMessage(SERVER_MODAL_MESSAGE_PLAYER_ID, senderCommand, messageText),
                        null,
                        true
                    );
                    log("[Server] Dispatched admin popup id=" + popup.id + " to onlinePlayers=" + USERS_BY_CONNECTION.size());
                } else {
                    log("[Server] Skipped empty admin popup id=" + popup.id);
                }
                DATABASE.markAdminPopupQueueDelivered(popup.id);
            } catch (Exception e) {
                log("[Server] Failed to dispatch admin popup id=" + popup.id + ": " + e.getMessage());
            }
        }
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
            handleGenericByteArrayMessage(connection, m);
            return;
        }

        if (msg instanceof LoginMessage) {
            LoginMessage m = (LoginMessage) msg;
            String requiredClientVersion = resolveRequiredClientVersion();
            if (!isAcceptedClientVersion(m.getVersion(), requiredClientVersion)) {
                sendGenericByteArraySerializable(
                    connection,
                    new LoggingInWithOldClientMessage(requiredClientVersion),
                    "LoggingInWithOldClientMessage"
                );
                log("[Server] Rejected login with old client version=" + safe(m.getVersion()) +
                    " requiredVersion=" + requiredClientVersion);
                return;
            }
            NordDatabase.LoginBlacklistRecord preAuthBlacklist =
                DATABASE.loadActiveLoginBlacklist(0, m.getUsername(), m.getMac());
            if (preAuthBlacklist != null) {
                sendPlayerBlacklisted(connection, preAuthBlacklist);
                log("[Server] Rejected blacklisted login by username/mac userName=" + safe(m.getUsername()));
                return;
            }
            NordDatabase.UserRecord user = DATABASE.authenticate(safe(m.getUsername()), safe(m.getPassword()));
            if (user == null) {
                sendLoginFailure(connection);
                return;
            }
            NordDatabase.LoginBlacklistRecord userBlacklist =
                DATABASE.loadActiveLoginBlacklist(user.userId, user.username, m.getMac());
            if (userBlacklist != null) {
                sendPlayerBlacklisted(connection, userBlacklist);
                log("[Server] Rejected blacklisted login for userId=" + user.userId);
                return;
            }
            Connection existingConnection = findConnectionByPlayerId(user.userId);
            if (existingConnection != null && existingConnection.getID() != connection.getID()) {
                sendGenericByteArraySerializable(
                    existingConnection,
                    new LoggedOutByNewLoginMessage(true),
                    "LoggedOutByNewLoginMessage"
                );
                existingConnection.close();
                log("[Server] Closed previous session for userId=" + user.userId +
                    " oldConnectionId=" + existingConnection.getID() +
                    " newConnectionId=" + connection.getID());
            }
            USERS_BY_CONNECTION.put(connection.getID(), user);
            maybePromoteExpiredBuildingCooldowns(user.villageId);
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

        if (msg instanceof FacebookConnectMessage) {
            FacebookConnectMessage m = (FacebookConnectMessage) msg;
            int responseCode = 1;
            NordDatabase.UserRecord linkedUser = USERS_BY_CONNECTION.get(connection.getID());
            String username = safe(m.getUsername());
            String password = safe(m.getPassword());
            if (!username.isEmpty() && !password.isEmpty()) {
                NordDatabase.UserRecord authenticated = DATABASE.authenticate(username, password);
                if (authenticated != null) {
                    linkedUser = authenticated;
                }
            }
            if (linkedUser != null) {
                NordDatabase.AccountProfileRecord existingProfile = DATABASE.loadAccountProfile(linkedUser.userId);
                try {
                    DATABASE.storeAccountProfile(
                        linkedUser.userId,
                        existingProfile != null ? existingProfile.firstName : linkedUser.username,
                        existingProfile != null ? existingProfile.surName : "",
                        existingProfile != null ? existingProfile.email : "",
                        existingProfile != null ? existingProfile.countryId : 0,
                        existingProfile != null ? existingProfile.birthDate : 0,
                        existingProfile != null ? existingProfile.birthMonth : 0,
                        existingProfile != null ? existingProfile.birthYear : 0,
                        existingProfile != null ? existingProfile.sex : 0,
                        existingProfile != null ? existingProfile.referralId : 0,
                        m.getFbUser(),
                        safe(m.getAccessToken())
                    );
                    responseCode = 0;
                } catch (IOException e) {
                    log("[Server] Failed to persist Facebook connect for userId=" + linkedUser.userId + ": " + e.getMessage());
                }
            }
            connection.sendTCP(new FacebookConnectResponseMessage(responseCode));
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
            try {
                DATABASE.storeAccountProfile(
                    result.user.userId,
                    safe(m.getFirstName()),
                    safe(m.getSurName()),
                    safe(m.getEmail()),
                    m.getCountryID(),
                    m.getBirthDate(),
                    m.getBirthMonth(),
                    m.getBirthYear(),
                    m.getSex(),
                    m.getReferralID(),
                    m.getFbUser(),
                    safe(m.getAccessToken())
                );
            } catch (IOException e) {
                log("[Server] Failed to store account profile for userId=" + result.user.userId + ": " + e.getMessage());
            }
            USERS_BY_CONNECTION.put(connection.getID(), result.user);
            int playerAge = DATABASE.getPlayerAgeYears(result.user.userId);
            connection.sendTCP(new CreateAccountResponseMessage(0, result.user.userId, true, playerAge));
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

        if (msg instanceof LogoutMessage) {
            logoutConnection(connection);
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

        if (msg instanceof CreateVillageMessage) {
            CreateVillageMessage m = (CreateVillageMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                connection.sendTCP(new CreateVillageResponseMessage(0));
                return;
            }
            String requestedVillageName = safe(m.getName()).trim();
            if (!requestedVillageName.isEmpty()) {
                try {
                    DATABASE.updateVillageName(user.userId, requestedVillageName);
                    refreshCachedUserRecord(user.userId);
                    user = DATABASE.findByUserId(user.userId);
                } catch (IOException e) {
                    log("[Server] Failed to update village name for userId=" + user.userId + ": " + e.getMessage());
                }
            }
            int villageId = user != null ? user.villageId : 0;
            connection.sendTCP(new CreateVillageResponseMessage(villageId));
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
            byte[] fallbackHeightMap = resolveHeightMapFallbackForVillage(villageId);
            NordDatabase.HeightMapRecord heightMap = DATABASE.loadHeightMap(villageId, fallbackHeightMap);
            connection.sendTCP(new RequestHeightMapResponseMessage(
                villageId,
                Arrays.copyOf(heightMap.heightData, heightMap.heightData.length),
                heightMap.type,
                heightMap.size
            ));
            return;
        }

        if (msg instanceof StoreHeightMapMessage) {
            StoreHeightMapMessage m = (StoreHeightMapMessage) msg;
            int villageId = authenticatedUser.villageId;
            if (villageId <= 0) {
                return;
            }
            byte[] heightData = safeBytes(m.getHeightData());
            try {
                DATABASE.storeHeightMap(villageId, heightData, m.getType(), m.getSize());
            } catch (IOException e) {
                log("[Server] Failed to store height map: " + e.getMessage());
            }
            return;
        }

        if (msg instanceof RequestBuildingsMessage) {
            RequestBuildingsMessage m = (RequestBuildingsMessage) msg;
            int villageId = resolveVillageId(connection, m.getVillageID());
            ensureVillageSubscription(connection, villageId, "RequestBuildings");
            ArrayList<Building> buildings = DATABASE.loadBuildings(villageId);
            if (villageId > 0) {
                LAST_BUILDINGS_SYNC_VILLAGE_BY_CONNECTION.put(connection.getID(), villageId);
            }
            connection.sendTCP(new RequestBuildingsResponseMessage(villageId, buildings, (byte) 1, (byte) 1, 0));
            return;
        }

        if (msg instanceof AddBuildingMessage) {
            AddBuildingMessage m = (AddBuildingMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            int villageId = resolveLiveVillageId(connection, m.getVillageID());
            if (villageId != 0 && !isConnectionInVillage(connection, villageId)) {
                joinVillage(connection, villageId);
            }
            Building building = m.getBuilding();
            if (user != null) {
                log("[Server] AddBuilding received userId=" + user.userId + " buildingId=" +
                    (building != null ? building.getBuildingID() : -1) +
                    " extraCredits=" + m.getExtraCredits() + " extraMessage=\"" + safe(m.getExtraMessage()) + "\"");
            }
            if (user == null || villageId == 0 || building == null) {
                int amount = user != null ? DATABASE.getSLXCredits(user.userId) : 0;
                connection.sendTCP(new RequestSLXCreditsResponseMessage(amount));
                return;
            }
            boolean isGiftBuildingType = BUILDING_GIFT_TYPE_IDS.contains(building.getBuildingType());
            if (villageId != user.villageId && !isGiftBuildingType) {
                log("[Server] Rejected cross-village AddBuilding economy write from userId=" + user.userId +
                    " villageId=" + villageId + " homeVillageId=" + user.villageId);
                sendBuildingAddRejected(connection, villageId, building.getBuildingID(), "cross-village");
                connection.sendTCP(new RequestSLXCreditsResponseMessage(DATABASE.getSLXCredits(user.userId)));
                return;
            }
            if (villageId != user.villageId) {
                log("[Server] Allowing cross-village AddBuilding gift purchase from userId=" + user.userId +
                    " villageId=" + villageId + " homeVillageId=" + user.villageId +
                    " buildingType=" + building.getBuildingType() + " buildingId=" + building.getBuildingID());
            }
            Integer basePriceValue = BUILDING_BASE_PRICE_BY_TYPE.get(building.getBuildingType());
            if (basePriceValue == null) {
                log("[Server] AddBuilding rejected unknown buildingType=" + building.getBuildingType() +
                    " userId=" + user.userId + " buildingId=" + building.getBuildingID());
                sendBuildingAddRejected(connection, villageId, building.getBuildingID(), "unknown-type");
                connection.sendTCP(new RequestSLXCreditsResponseMessage(DATABASE.getSLXCredits(user.userId)));
                return;
            }
            int basePrice = Math.max(0, basePriceValue);
            int extraCredits = Math.max(0, m.getExtraCredits());
            int totalCost = clampToInt((long) basePrice + extraCredits);
            try {
                NordDatabase.AddBuildingPurchaseResult result =
                    DATABASE.addBuildingWithCreditSpend(user.userId, villageId, building, totalCost, isGiftBuildingType);
                refreshCachedUserRecord(user.userId);
                connection.sendTCP(new RequestSLXCreditsResponseMessage(result.creditsAfter));
                if (!result.added) {
                    if (result.insufficientCredits) {
                        log("[Server] AddBuilding denied insufficient credits userId=" + user.userId +
                            " buildingId=" + building.getBuildingID() +
                            " totalCost=" + totalCost +
                            " creditsAfter=" + result.creditsAfter);
                        sendBuildingAddRejected(connection, villageId, building.getBuildingID(), "insufficient-credits");
                    } else {
                        log("[Server] AddBuilding rejected userId=" + user.userId +
                            " villageId=" + villageId +
                            " buildingId=" + building.getBuildingID());
                        sendBuildingAddRejected(connection, villageId, building.getBuildingID(), "db-rejected");
                    }
                    return;
                }
                sendGenericByteArraySerializable(
                    connection,
                    new BuildingAddedMessage(villageId, building.getBuildingID()),
                    "BuildingAddedMessage"
                );
                // Placing client already applies the new building locally; keep notification broadcast only for other players.
                broadcastToVillage(villageId, new BuildingAddedNotificationMessage(villageId, building), connection, false, false);
            } catch (IOException e) {
                log("[Server] Failed to store building + charge credits: " + e.getMessage());
                sendBuildingAddRejected(connection, villageId, building.getBuildingID(), "io-failure");
                connection.sendTCP(new RequestSLXCreditsResponseMessage(DATABASE.getSLXCredits(user.userId)));
            }
            return;
        }

        if (msg instanceof UpdateBuildingMessage) {
            UpdateBuildingMessage m = (UpdateBuildingMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            int villageId = resolveLiveVillageId(connection, m.getVillageID());
            if (user == null || villageId == 0) {
                return;
            }
            if (villageId != user.villageId) {
                log("[Server] Rejected cross-village UpdateBuilding write from userId=" + user.userId +
                    " villageId=" + villageId + " homeVillageId=" + user.villageId +
                    " buildingId=" + m.getBuildingID());
                return;
            }
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
            if (user != null) {
                rebroadcastAvatarPositionIfKnown(villageId, user.userId, connection);
            }
            return;
        }

        if (msg instanceof RemoveBuildingMessage) {
            RemoveBuildingMessage m = (RemoveBuildingMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            int villageId = resolveLiveVillageId(connection, m.getVillageID());
            if (villageId != 0 && !isConnectionInVillage(connection, villageId)) {
                joinVillage(connection, villageId);
            }
            if (user == null || villageId == 0) {
                int amount = user != null ? DATABASE.getSLXCredits(user.userId) : 0;
                connection.sendTCP(new RequestSLXCreditsResponseMessage(amount));
                return;
            }
            if (villageId != user.villageId) {
                log("[Server] Rejected cross-village RemoveBuilding economy write from userId=" + user.userId +
                    " villageId=" + villageId + " homeVillageId=" + user.villageId);
                connection.sendTCP(new RequestSLXCreditsResponseMessage(DATABASE.getSLXCredits(user.userId)));
                return;
            }
            try {
                Short buildingTypeHint = DATABASE.loadBuildingType(villageId, m.getBuildingID());
                boolean likelyTreasureChest =
                    buildingTypeHint != null && BUILDING_TREASURE_TYPE_BY_TYPE.containsKey(buildingTypeHint);
                int requestedRefund = likelyTreasureChest ? 0 : m.getPrice();
                NordDatabase.RemoveBuildingRefundResult result =
                    DATABASE.removeBuildingWithCreditRefund(user.userId, villageId, m.getBuildingID(), requestedRefund);
                int creditsAfter = result.creditsAfter;
                TreasurePickupReward treasureReward = null;
                if (result.removed) {
                    TreasureType configuredTreasureType = BUILDING_TREASURE_TYPE_BY_TYPE.get(result.buildingType);
                    if (configuredTreasureType != null) {
                        // Treasure chests never refund bulldoze credits; normalize if a forged client requested one.
                        if (result.refundGranted > 0) {
                            creditsAfter = DATABASE.applySLXCreditDelta(user.userId, -result.refundGranted);
                        }
                        treasureReward = rollTreasureReward(configuredTreasureType, result.buildingType);
                        if (treasureReward.treasureType == TreasureType.COINS && treasureReward.coinsInTreasure > 0) {
                            creditsAfter = DATABASE.applySLXCreditDelta(user.userId, treasureReward.coinsInTreasure);
                        }
                    }
                }
                refreshCachedUserRecord(user.userId);
                connection.sendTCP(new RequestSLXCreditsResponseMessage(creditsAfter));
                if (!result.removed) {
                    return;
                }
                if (treasureReward != null) {
                    sendPickedUpTreasureMessage(connection, treasureReward.treasureType, creditsAfter, treasureReward.coinsInTreasure);
                    log("[Server] RemoveBuilding picked treasure userId=" + user.userId +
                        " buildingId=" + m.getBuildingID() +
                        " buildingType=" + result.buildingType +
                        " treasureType=" + treasureReward.treasureType +
                        " treasureAmount=" + treasureReward.coinsInTreasure +
                        " creditsAfter=" + creditsAfter);
                } else {
                    log("[Server] RemoveBuilding refunded userId=" + user.userId +
                        " buildingId=" + m.getBuildingID() +
                        " buildingType=" + result.buildingType +
                        " requestedRefund=" + Math.max(0, m.getPrice()) +
                        " grantedRefund=" + result.refundGranted);
                }
                broadcastToVillage(villageId, new BuildingRemovedNotificationMessage(villageId, m.getBuildingID()), null, true, false);
            } catch (IOException e) {
                log("[Server] Failed to remove building + refund credits: " + e.getMessage());
                connection.sendTCP(new RequestSLXCreditsResponseMessage(DATABASE.getSLXCredits(user.userId)));
            }
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
            connection.sendTCP(new RequestCharactersResponseMessage(villageId, DATABASE.loadCharacters(villageId)));
            return;
        }

        if (msg instanceof AddCharacterMessage) {
            AddCharacterMessage m = (AddCharacterMessage) msg;
            int villageId = resolveLiveVillageId(connection, m.getVillageID());
            if (villageId == 0) {
                return;
            }
            if (villageId != 0 && !isConnectionInVillage(connection, villageId)) {
                joinVillage(connection, villageId);
            }
            if (m.getCharacter() == null) {
                return;
            }
            try {
                DATABASE.upsertCharacter(villageId, m.getCharacter());
            } catch (IOException e) {
                log("[Server] Failed to add character: " + e.getMessage());
            }
            broadcastToVillage(villageId, new AddCharacterNotificationMessage(villageId, m.getCharacter()), connection, false, false);
            return;
        }

        if (msg instanceof UpdateCharacterMessage) {
            UpdateCharacterMessage m = (UpdateCharacterMessage) msg;
            int villageId = resolveLiveVillageId(connection, m.getVillageID());
            if (villageId == 0) {
                return;
            }
            if (villageId != 0 && !isConnectionInVillage(connection, villageId)) {
                joinVillage(connection, villageId);
            }
            if (m.getCharacter() == null) {
                return;
            }
            try {
                DATABASE.upsertCharacter(villageId, m.getCharacter());
            } catch (IOException e) {
                log("[Server] Failed to update character: " + e.getMessage());
            }
            broadcastToVillage(villageId, new CharacterUpdatedNotificationMessage(villageId, m.getCharacter()), connection, false, false);
            return;
        }

        if (msg instanceof UpdateCharactersMessage) {
            UpdateCharactersMessage m = (UpdateCharactersMessage) msg;
            int villageId = resolveLiveVillageId(connection, m.getVillageID());
            if (villageId == 0) {
                return;
            }
            if (villageId != 0 && !isConnectionInVillage(connection, villageId)) {
                joinVillage(connection, villageId);
            }
            try {
                DATABASE.upsertCharacters(villageId, m.getCharacters());
            } catch (IOException e) {
                log("[Server] Failed to update characters: " + e.getMessage());
            }
            broadcastToVillage(villageId, new CharactersUpdatedNotificationMessage(villageId, m.getCharacters()), connection, false, false);
            return;
        }

        if (msg instanceof RemoveCharacterMessage) {
            RemoveCharacterMessage m = (RemoveCharacterMessage) msg;
            int villageId = resolveLiveVillageId(connection, m.getVillageID());
            if (villageId == 0) {
                return;
            }
            if (villageId != 0 && !isConnectionInVillage(connection, villageId)) {
                joinVillage(connection, villageId);
            }
            try {
                DATABASE.removeCharacter(villageId, m.getCharacterID());
            } catch (IOException e) {
                log("[Server] Failed to remove character: " + e.getMessage());
            }
            broadcastToVillage(villageId, new RemoveCharacterNotificationMessage(villageId, m.getCharacterID()), null, true, false);
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
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                return;
            }
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
            rebroadcastAvatarPositionIfKnown(villageId, user.userId, connection);
            return;
        }

        if (msg instanceof UpdateIngredientsMessage) {
            UpdateIngredientsMessage m = (UpdateIngredientsMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                return;
            }
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
            rebroadcastAvatarPositionIfKnown(villageId, user.userId, connection);
            return;
        }

        if (msg instanceof ConsumeMessage) {
            ConsumeMessage m = (ConsumeMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                return;
            }
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
            rebroadcastAvatarPositionIfKnown(villageId, user.userId, connection);
            return;
        }

        if (msg instanceof SendTextToVillageMessage) {
            SendTextToVillageMessage m = (SendTextToVillageMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                return;
            }
            if (tryHandleSlashCommand(connection, user, m.getWhatToSay())) {
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

        if (msg instanceof SendLiveMessage) {
            SendLiveMessage m = (SendLiveMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                return;
            }
            String text = safe(m.getText());
            if (text.trim().isEmpty()) {
                return;
            }
            byte playerSex = DATABASE.getPlayerSex(user.userId);
            int playerAgeYears = DATABASE.getPlayerAgeYears(user.userId);
            byte playerAge = (byte) Math.max(0, Math.min(127, playerAgeYears));
            LiveNotificationMessage notification = new LiveNotificationMessage(
                user.userId,
                m.getSignType(),
                user.username,
                text,
                "",
                playerSex,
                playerAge,
                user.level
            );
            // Sender has already rendered this message locally before the network round-trip.
            broadcastToOnline(notification, connection, false);
            return;
        }

        if (msg instanceof SendTextToPlayerMessage) {
            SendTextToPlayerMessage m = (SendTextToPlayerMessage) msg;
            NordDatabase.UserRecord fromUser = USERS_BY_CONNECTION.get(connection.getID());
            if (fromUser == null) {
                return;
            }
            if (tryHandleSlashCommand(connection, fromUser, m.getWhatToSay())) {
                return;
            }
            int targetPlayerId = m.getPlayerID();
            if (targetPlayerId <= 0 || targetPlayerId == fromUser.userId) {
                return;
            }
            if (DATABASE.isPlayerBlocked(targetPlayerId, fromUser.userId)) {
                sendGenericByteArraySerializable(
                    connection,
                    new PlayerHasYouBlockedMessage(targetPlayerId),
                    "PlayerHasYouBlockedMessage"
                );
                return;
            }
            Connection toConnection = findConnectionByPlayerId(targetPlayerId);
            if (toConnection != null) {
                toConnection.sendTCP(new SendTextToPlayerNotificationMessage(fromUser.userId, fromUser.username, safe(m.getWhatToSay())));
            }
            return;
        }

        if (msg instanceof SendEventToPlayerMessage) {
            SendEventToPlayerMessage m = (SendEventToPlayerMessage) msg;
            int targetPlayerId = m.getPlayerID();
            if (targetPlayerId <= 0) {
                return;
            }
            Connection targetConnection = findConnectionByPlayerId(targetPlayerId);
            if (targetConnection != null) {
                targetConnection.sendTCP(new ReceiveEventMessage(m.getEventObject()));
            }
            return;
        }

        if (msg instanceof AddEventMessage) {
            AddEventMessage m = (AddEventMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                return;
            }
            int player2Id = clampToInt(Math.max(0L, m.getPlayer2()));
            try {
                DATABASE.addEvent(user.userId, player2Id, m.getType(), safeBytes(m.getData()));
            } catch (IOException e) {
                log("[Server] Failed to store event from playerId=" + user.userId + ": " + e.getMessage());
            }
            return;
        }

        if (msg instanceof RequestEventsMessage) {
            RequestEventsMessage m = (RequestEventsMessage) msg;
            int playerId = resolvePlayerId(connection, m.getPlayerID());
            if (playerId == 0) {
                return;
            }
            NordDatabase.EventPage page = DATABASE.loadEventsForPlayer(playerId, m.getOffset(), m.getAmount());
            try {
                connection.sendTCP(
                    com.slx.nord.jgnmessages.clientinbound.GenericByteArrayMessage.createSerializedByteArrayMessage(
                        new com.slx.nord.jgnmessages.bytearraymessages.RequestEventsResponseMessage(
                            page.events,
                            Math.max(0, m.getOffset()),
                            page.olderEventsAvailable
                        )
                    )
                );
            } catch (Exception e) {
                log("[Server] Failed to serialize events response payload, falling back to empty list: " + e.getMessage());
                try {
                    connection.sendTCP(
                        com.slx.nord.jgnmessages.clientinbound.GenericByteArrayMessage.createSerializedByteArrayMessage(
                            new com.slx.nord.jgnmessages.bytearraymessages.RequestEventsResponseMessage(
                                new ArrayList<>(),
                                Math.max(0, m.getOffset()),
                                false
                            )
                        )
                    );
                } catch (Exception fallbackException) {
                    log("[Server] Failed to send fallback events response: " + fallbackException.getMessage());
                }
            }
            return;
        }

        if (msg instanceof RequestIMListMessage) {
            RequestIMListMessage m = (RequestIMListMessage) msg;
            int playerId = resolvePlayerId(connection, m.getPlayerID());
            sendIMListSnapshot(connection, playerId);
            return;
        }

        if (msg instanceof AddToIMListMessage) {
            AddToIMListMessage m = (AddToIMListMessage) msg;
            NordDatabase.UserRecord currentUser = USERS_BY_CONNECTION.get(connection.getID());
            int playerId = currentUser != null ? currentUser.userId : 0;
            int otherPlayerId = m.getOtherPlayer();
            if (playerId <= 0 || otherPlayerId <= 0 || playerId == otherPlayerId) {
                return;
            }
            if (DATABASE.findByUserId(otherPlayerId) == null) {
                return;
            }
            try {
                // Keep IM list symmetric so either player's add action creates a shared friendship.
                DATABASE.addIMListEntry(playerId, otherPlayerId);
                DATABASE.addIMListEntry(otherPlayerId, playerId);
                sendIMListSnapshot(connection, playerId);
                Connection otherConnection = findConnectionByPlayerId(otherPlayerId);
                if (otherConnection != null) {
                    sendIMListSnapshot(otherConnection, otherPlayerId);
                }
            } catch (IOException e) {
                log("[Server] Failed to add IM list entry: " + e.getMessage());
            }
            return;
        }

        if (msg instanceof RemoveFromIMListMessage) {
            RemoveFromIMListMessage m = (RemoveFromIMListMessage) msg;
            NordDatabase.UserRecord currentUser = USERS_BY_CONNECTION.get(connection.getID());
            int playerId = currentUser != null ? currentUser.userId : 0;
            int otherPlayerId = m.getOtherPlayer();
            if (playerId <= 0 || otherPlayerId <= 0 || playerId == otherPlayerId) {
                return;
            }
            try {
                DATABASE.removeIMListEntry(playerId, otherPlayerId);
                DATABASE.removeIMListEntry(otherPlayerId, playerId);
                sendIMListSnapshot(connection, playerId);
                Connection otherConnection = findConnectionByPlayerId(otherPlayerId);
                if (otherConnection != null) {
                    sendIMListSnapshot(otherConnection, otherPlayerId);
                }
            } catch (IOException e) {
                log("[Server] Failed to remove IM list entry: " + e.getMessage());
            }
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

        if (msg instanceof AddGuestbookEntryMessage) {
            AddGuestbookEntryMessage m = (AddGuestbookEntryMessage) msg;
            NordDatabase.UserRecord currentUser = USERS_BY_CONNECTION.get(connection.getID());
            if (currentUser == null) {
                return;
            }
            int ownerPlayerId = resolveReadablePlayerId(connection, m.getPlayerID());
            if (ownerPlayerId <= 0) {
                return;
            }
            try {
                DATABASE.addGuestbookEntry(ownerPlayerId, currentUser.userId, safe(m.getText()), m.isPrivate());
                notifyGuestbookEntryAdded(ownerPlayerId, currentUser.userId, currentUser.username);
            } catch (IOException e) {
                log("[Server] Failed to add guestbook entry: " + e.getMessage());
            }
            return;
        }

        if (msg instanceof RemoveGuestbookMessage) {
            RemoveGuestbookMessage m = (RemoveGuestbookMessage) msg;
            NordDatabase.UserRecord currentUser = USERS_BY_CONNECTION.get(connection.getID());
            if (currentUser == null) {
                return;
            }
            try {
                DATABASE.removeGuestbookEntry(m.getGuestbookMessage(), currentUser.userId);
            } catch (IOException e) {
                log("[Server] Failed to remove guestbook entry: " + e.getMessage());
            }
            return;
        }

        if (msg instanceof SetGuestbookMessagePrivateMessage) {
            SetGuestbookMessagePrivateMessage m = (SetGuestbookMessagePrivateMessage) msg;
            NordDatabase.UserRecord currentUser = USERS_BY_CONNECTION.get(connection.getID());
            if (currentUser == null) {
                return;
            }
            int ownerPlayerId = resolveReadablePlayerId(connection, m.getPlayerID());
            try {
                DATABASE.setGuestbookEntryPrivate(ownerPlayerId, m.getPostID(), currentUser.userId, m.isSetPrivate());
            } catch (IOException e) {
                log("[Server] Failed to update guestbook privacy: " + e.getMessage());
            }
            return;
        }

        if (msg instanceof RequestGuestbookEntriesMessage) {
            RequestGuestbookEntriesMessage m = (RequestGuestbookEntriesMessage) msg;
            int ownerPlayerId = resolveReadablePlayerId(connection, m.getPlayerID());
            NordDatabase.UserRecord owner = DATABASE.findByUserId(ownerPlayerId);
            String ownerName = owner != null ? owner.username : ("Player " + ownerPlayerId);
            NordDatabase.UserRecord viewer = USERS_BY_CONNECTION.get(connection.getID());
            int viewerPlayerId = viewer != null ? viewer.userId : 0;
            NordDatabase.GuestbookPage page = DATABASE.loadGuestbookEntries(
                ownerPlayerId,
                viewerPlayerId,
                m.getOffset(),
                m.getNrOfEntries()
            );
            connection.sendTCP(buildGuestbookEntriesResponse(ownerPlayerId, ownerName, page));
            return;
        }

        if (msg instanceof RequestGuestbookHistory) {
            RequestGuestbookHistory m = (RequestGuestbookHistory) msg;
            NordDatabase.UserRecord viewer = USERS_BY_CONNECTION.get(connection.getID());
            int viewerPlayerId = viewer != null ? viewer.userId : 0;
            NordDatabase.GuestbookPage page = DATABASE.loadGuestbookHistory(
                m.getPlayer1(),
                m.getPlayer2(),
                viewerPlayerId,
                m.getOffset(),
                m.getNrEntries()
            );
            connection.sendTCP(buildGuestbookHistoryResponse(m.getOffset(), m.getPlayer1(), m.getPlayer2(), page));
            return;
        }

        if (msg instanceof RequestImageMessage) {
            RequestImageMessage m = (RequestImageMessage) msg;
            byte[] imageData = DATABASE.loadUploadedPictureBytes(m.getImageId(), m.isThumbnail());
            connection.sendTCP(new RequestImageResponseMessage(m.getImageId(), m.isThumbnail(), imageData));
            return;
        }

        if (msg instanceof RequestBadgeDataMessage) {
            RequestBadgeDataMessage m = (RequestBadgeDataMessage) msg;
            int playerId = resolveReadablePlayerId(connection, m.getPlayerID());
            com.nordclientpc.eM badgeData = buildBadgeDataForPlayer(playerId);
            connection.sendTCP(new RequestBadgeDataResponseMessage(playerId, badgeData));
            return;
        }

        if (msg instanceof RequestMidiMessage) {
            RequestMidiMessage m = (RequestMidiMessage) msg;
            NordDatabase.MidiRecord midi = DATABASE.loadMidi(m.getSongID());
            int playerId = midi != null ? midi.playerId : 0;
            byte[] data = midi != null ? safeBytes(midi.data) : new byte[0];
            boolean isGift = midi != null && midi.isGift;
            connection.sendTCP(new RequestMidiResponseMessage(playerId, m.getSongID(), data, isGift));
            return;
        }

        if (msg instanceof StoreMidiMessage) {
            StoreMidiMessage m = (StoreMidiMessage) msg;
            int playerId = resolvePlayerId(connection, m.getPlayerID());
            if (playerId == 0) {
                connection.sendTCP(new StoreMidiResponseMessage(0, false, 0L));
                return;
            }
            try {
                long songId = DATABASE.storeMidi(playerId, m.isIsGift(), safeBytes(m.getData()));
                connection.sendTCP(new StoreMidiResponseMessage(playerId, songId > 0L, songId));
            } catch (IOException e) {
                log("[Server] Failed to store midi for playerId=" + playerId + ": " + e.getMessage());
                connection.sendTCP(new StoreMidiResponseMessage(playerId, false, 0L));
            }
            return;
        }

        if (msg instanceof StorePointWalkAnswerMessage) {
            StorePointWalkAnswerMessage m = (StorePointWalkAnswerMessage) msg;
            int villageId = resolveLiveVillageId(connection, m.getVillageID());
            int playerId = resolvePlayerId(connection, 0);
            if (villageId == 0 || playerId == 0) {
                return;
            }
            try {
                DATABASE.storePointWalkAnswer(
                    villageId,
                    playerId,
                    m.getBuildingThatContainsQuestionID(),
                    m.getTheAnswer(),
                    m.isWasCorrectAnswer()
                );
            } catch (IOException e) {
                log("[Server] Failed to store pointwalk answer for playerId=" + playerId + ": " + e.getMessage());
            }
            return;
        }

        if (msg instanceof RemoveAllPointWalkAnswersInOwnVillageMessage) {
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                return;
            }
            int villageId = user.villageId;
            if (villageId <= 0) {
                return;
            }
            try {
                DATABASE.clearPointWalkAnswersInVillage(villageId);
                broadcastToVillage(
                    villageId,
                    new PointWalkAnswersRemovedNotificationMessage(villageId),
                    null,
                    true,
                    false
                );
            } catch (IOException e) {
                log("[Server] Failed to clear pointwalk answers for villageId=" + villageId + ": " + e.getMessage());
            }
            return;
        }

        if (msg instanceof RequestPointWalkQuestionStatisticsMessage) {
            RequestPointWalkQuestionStatisticsMessage m = (RequestPointWalkQuestionStatisticsMessage) msg;
            int villageId = resolveVillageId(connection, m.getVillageID());
            NordDatabase.PointWalkQuestionStatsRecord stats =
                DATABASE.loadPointWalkQuestionStatistics(villageId, m.getBuildingThatContainsQuestionID());
            connection.sendTCP(new RequestPointWalkQuestionStatisticsResponseMessage(
                villageId,
                m.getBuildingThatContainsQuestionID(),
                stats.nrCorrectAnswers,
                stats.nrWrongAnswers,
                stats.nr1s,
                stats.nrXs,
                stats.nr2s
            ));
            return;
        }

        if (msg instanceof RequestPointWalkVillageAnswersForPlayerMessage) {
            RequestPointWalkVillageAnswersForPlayerMessage m = (RequestPointWalkVillageAnswersForPlayerMessage) msg;
            int villageId = resolveVillageId(connection, m.getVillageID());
            int playerId = m.getToPlayerID();
            if (playerId <= 0) {
                playerId = resolvePlayerId(connection, 0);
            }
            ArrayList<Short> questionIds = DATABASE.loadPointWalkQuestionIdsForPlayer(villageId, playerId);
            ArrayList<Byte> answers = DATABASE.loadPointWalkAnswersForPlayer(villageId, playerId);
            connection.sendTCP(new RequestPointWalkVillageAnswersForPlayerResponseMessage(
                villageId,
                questionIds,
                answers
            ));
            return;
        }

        if (msg instanceof RequestPointWalkVillageHighscoreMessage) {
            RequestPointWalkVillageHighscoreMessage m = (RequestPointWalkVillageHighscoreMessage) msg;
            int villageId = resolveVillageId(connection, m.getVillageID());
            ArrayList<NordDatabase.PointWalkHighscoreRecord> highscores = DATABASE.loadPointWalkVillageHighscore(villageId, 20);
            ArrayList<Integer> playerIds = new ArrayList<>();
            ArrayList<String> playerNames = new ArrayList<>();
            ArrayList<Integer> playerScores = new ArrayList<>();
            for (NordDatabase.PointWalkHighscoreRecord score : highscores) {
                playerIds.add(score.playerId);
                playerNames.add(score.playerName);
                playerScores.add(score.score);
            }
            connection.sendTCP(new RequestPointWalkVillageHighscoreResponseMessage(
                villageId,
                playerIds,
                playerNames,
                playerScores
            ));
            return;
        }

        if (msg instanceof RequestPlayerClothesMessage) {
            RequestPlayerClothesMessage m = (RequestPlayerClothesMessage) msg;
            int playerId = resolveReadablePlayerId(connection, m.getPlayerID());
            ArrayList clothes = DATABASE.loadPlayerClothes(playerId);
            log("[Server] Responding player clothes playerId=" + playerId + " count=" + clothes.size());
            connection.sendTCP(new RequestPlayerClothesResponseMessage(playerId, clothes));
            return;
        }

        if (msg instanceof StorePlayerClothesMessage) {
            StorePlayerClothesMessage m = (StorePlayerClothesMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                return;
            }
            try {
                DATABASE.storePlayerClothes(user.userId, m.getClothes());
                connection.sendTCP(new RequestPlayerClothesResponseMessage(user.userId, DATABASE.loadPlayerClothes(user.userId)));
            } catch (IOException e) {
                log("[Server] Failed to store player clothes: " + e.getMessage());
            }
            return;
        }

        if (msg instanceof RequestPlayerImagesMessage) {
            RequestPlayerImagesMessage m = (RequestPlayerImagesMessage) msg;
            int playerId = resolveReadablePlayerId(connection, m.getPlayerID());
            ArrayList<Integer> images = DATABASE.loadUploadedPictureIds(playerId, DEFAULT_PLAYER_IMAGES_LIMIT);
            connection.sendTCP(new RequestPlayerImagesResonseMessage(playerId, resolvePlayerImagesPrefix(), images));
            return;
        }

        if (msg instanceof RequestQuestsMessage) {
            RequestQuestsMessage m = (RequestQuestsMessage) msg;
            int playerId = resolvePlayerId(connection, m.getPlayerID());
            if (playerId == 0) {
                return;
            }
            ArrayList<NordDatabase.QuestRecord> quests = DATABASE.loadQuests(playerId);
            ArrayList<Long> ids = new ArrayList<>();
            ArrayList<Integer> targetPlayerIds = new ArrayList<>();
            ArrayList<Short> types = new ArrayList<>();
            ArrayList<byte[]> datas = new ArrayList<>();
            ArrayList<Boolean> finished = new ArrayList<>();
            for (NordDatabase.QuestRecord quest : quests) {
                ids.add(quest.questId);
                targetPlayerIds.add(quest.targetPlayerId);
                types.add(quest.type);
                datas.add(safeBytes(quest.data));
                finished.add(quest.finished);
            }
            connection.sendTCP(new RequestQuestsResponseMessage(
                playerId,
                ids,
                targetPlayerIds,
                types,
                datas,
                finished
            ));
            return;
        }

        if (msg instanceof StoreQuestMessage) {
            StoreQuestMessage m = (StoreQuestMessage) msg;
            int playerId = resolvePlayerId(connection, m.getPlayerID());
            if (playerId == 0) {
                return;
            }
            try {
                DATABASE.storeQuest(
                    playerId,
                    m.getTargetPlayerID(),
                    m.getType(),
                    safeBytes(m.getData()),
                    m.isFinished()
                );
            } catch (IOException e) {
                log("[Server] Failed to store quest for playerId=" + playerId + ": " + e.getMessage());
            }
            return;
        }

        if (msg instanceof FinishedQuestMessage) {
            FinishedQuestMessage m = (FinishedQuestMessage) msg;
            int playerId = resolvePlayerId(connection, m.getPlayerID());
            if (playerId == 0) {
                return;
            }
            try {
                DATABASE.finishQuestsByType(playerId, m.getType());
            } catch (IOException e) {
                log("[Server] Failed to finish quest type=" + m.getType() + " for playerId=" + playerId + ": " + e.getMessage());
            }
            return;
        }

        if (msg instanceof RemoveQuestMessage) {
            RemoveQuestMessage m = (RemoveQuestMessage) msg;
            int playerId = resolvePlayerId(connection, 0);
            if (playerId == 0) {
                return;
            }
            try {
                DATABASE.removeQuest(playerId, m.getQuestID());
            } catch (IOException e) {
                log("[Server] Failed to remove questId=" + m.getQuestID() + " for playerId=" + playerId + ": " + e.getMessage());
            }
            return;
        }

        if (msg instanceof StoreShareMessage) {
            StoreShareMessage m = (StoreShareMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                return;
            }
            long shareId = 0L;
            try {
                shareId = DATABASE.storeShare(user.userId, m.getShareType(), m.getShareData());
            } catch (IOException e) {
                log("[Server] Failed to store share for playerId=" + user.userId + ": " + e.getMessage());
            }
            String shareAction = buildShareAction(user.userId, m.getShareType(), m.getShareData(), shareId);
            connection.sendTCP(new StoreShareResponseMessage(user.userId, shareAction));
            return;
        }

        if (msg instanceof SubmitReportMessage) {
            SubmitReportMessage m = (SubmitReportMessage) msg;
            NordDatabase.UserRecord reporter = USERS_BY_CONNECTION.get(connection.getID());
            if (reporter == null) {
                return;
            }
            int reportedPlayerId = Math.max(0, m.getPlayerID());
            try {
                DATABASE.storeReport(reporter.userId, reportedPlayerId, m.getType(), safeBytes(m.getData()));
            } catch (IOException e) {
                log("[Server] Failed to store report from playerId=" + reporter.userId + ": " + e.getMessage());
            }
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
                CompanionPurchaseRequest companionPurchase = parseCompanionPurchaseRequest(m.getLogMessage());
                int currentCredits = DATABASE.getSLXCredits(user.userId);
                if (companionPurchase != null) {
                    CompanionCatalogEntry entry = resolveCompanionCatalogEntry(companionPurchase.kind, companionPurchase.companionType);
                    if (entry == null) {
                        connection.sendTCP(new RequestSLXCreditsResponseMessage(currentCredits));
                        log("[Server] Rejected companion purchase spend for userId=" + user.userId +
                            " unknown " + companionPurchase.kind + " type=" + companionPurchase.companionType);
                        return;
                    }
                    if (entry.price <= 0) {
                        connection.sendTCP(new RequestSLXCreditsResponseMessage(currentCredits));
                        log("[Server] Ignored zero-price companion purchase spend for userId=" + user.userId +
                            " " + companionPurchase.kind + " type=" + companionPurchase.companionType);
                        return;
                    }
                    if (m.getAmount() < entry.price) {
                        connection.sendTCP(new RequestSLXCreditsResponseMessage(currentCredits));
                        log("[Server] Rejected companion purchase spend for userId=" + user.userId +
                            " " + companionPurchase.kind + " type=" + companionPurchase.companionType +
                            " amount=" + m.getAmount() + " price=" + entry.price);
                        return;
                    }
                    if (user.level < entry.requiredLevel) {
                        connection.sendTCP(new RequestSLXCreditsResponseMessage(currentCredits));
                        log("[Server] Rejected companion purchase spend for userId=" + user.userId +
                            " " + companionPurchase.kind + " type=" + companionPurchase.companionType +
                            " due to level requirement " + entry.requiredLevel);
                        return;
                    }
                    if (!isCompanionSeasonAvailable(entry, LocalDate.now())) {
                        connection.sendTCP(new RequestSLXCreditsResponseMessage(currentCredits));
                        log("[Server] Rejected companion purchase spend for userId=" + user.userId +
                            " " + companionPurchase.kind + " type=" + companionPurchase.companionType +
                            " outside sale window");
                        return;
                    }
                    if (DATABASE.isCompanionOwned(user.userId, companionPurchase.kind, companionPurchase.companionType)) {
                        connection.sendTCP(new RequestSLXCreditsResponseMessage(currentCredits));
                        log("[Server] Ignored already-owned companion purchase for userId=" + user.userId +
                            " " + companionPurchase.kind + " type=" + companionPurchase.companionType);
                        return;
                    }
                    AvatarDetails currentAvatar = DATABASE.loadAvatarDetails(user.userId, user.username, user.level);
                    short equippedType = "pet".equals(companionPurchase.kind)
                        ? currentAvatar.getPetType()
                        : currentAvatar.getMountType();
                    if (equippedType == companionPurchase.companionType) {
                        DATABASE.grantCompanionOwnership(user.userId, companionPurchase.kind, companionPurchase.companionType, "avatar_state");
                        connection.sendTCP(new RequestSLXCreditsResponseMessage(currentCredits));
                        log("[Server] Backfilled companion ownership for userId=" + user.userId +
                            " " + companionPurchase.kind + " type=" + companionPurchase.companionType +
                            " from equipped avatar state");
                        return;
                    }
                    if (currentCredits < entry.price) {
                        connection.sendTCP(new RequestSLXCreditsResponseMessage(currentCredits));
                        log("[Server] Rejected companion purchase spend for userId=" + user.userId +
                            " insufficient credits current=" + currentCredits + " price=" + entry.price);
                        return;
                    }
                }
                int amount = DATABASE.spendSLXCredits(user.userId, m.getAmount());
                if (companionPurchase != null && amount < currentCredits) {
                    DATABASE.grantCompanionOwnership(user.userId, companionPurchase.kind, companionPurchase.companionType, safe(m.getLogMessage()));
                }
                refreshCachedUserRecord(user.userId);
                connection.sendTCP(new RequestSLXCreditsResponseMessage(amount));
            } catch (IOException e) {
                log("[Server] Failed to spend SLX credits: " + e.getMessage());
            }
            return;
        }

        if (msg instanceof BuyCreditsMessage) {
            BuyCreditsMessage m = (BuyCreditsMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                connection.sendTCP(new BuyCreditsResponseMessage(0, -1));
                return;
            }
            if (!isValidServerSession(connection, m.getSessionID())) {
                int credits = DATABASE.getSLXCredits(user.userId);
                connection.sendTCP(new BuyCreditsResponseMessage(credits, -1));
                log("[Server] Rejected BuyCreditsMessage from userId=" + user.userId + " due to invalid session id");
                return;
            }
            if (REQUIRE_PROVIDER_CALLBACK_BUY_CREDITS) {
                int currentCredits = DATABASE.getSLXCredits(user.userId);
                connection.sendTCP(new BuyCreditsResponseMessage(currentCredits, 0));
                log("[Server] BuyCreditsMessage ignored for userId=" + user.userId +
                    " because provider callback mode is enabled");
                return;
            }
            int boughtCredits = resolveBuyCreditsAmount(m.getCode(), m.getLogMessage());
            if (boughtCredits <= 0) {
                int currentCredits = DATABASE.getSLXCredits(user.userId);
                connection.sendTCP(new BuyCreditsResponseMessage(currentCredits, -1));
                log("[Server] Rejected BuyCreditsMessage from userId=" + user.userId +
                    " code=\"" + safe(m.getCode()) + "\" log=\"" + safe(m.getLogMessage()) + "\"");
                return;
            }
            try {
                int totalCredits = DATABASE.applySLXCreditDelta(user.userId, boughtCredits);
                refreshCachedUserRecord(user.userId);
                connection.sendTCP(new BuyCreditsResponseMessage(totalCredits, boughtCredits));
                sendBuyCreditsNotification(connection, user, boughtCredits, totalCredits, m.getCode(), m.getLogMessage());
                log("[Server] BuyCredits applied userId=" + user.userId +
                    " bought=" + boughtCredits +
                    " total=" + totalCredits +
                    " source=" + resolveBuyCreditsSource(m.getCode(), m.getLogMessage()));
            } catch (IOException e) {
                int fallbackTotal = DATABASE.getSLXCredits(user.userId);
                connection.sendTCP(new BuyCreditsResponseMessage(fallbackTotal, -1));
                log("[Server] Failed to apply BuyCreditsMessage for userId=" + user.userId + ": " + e.getMessage());
            }
            return;
        }

        if (msg instanceof RequestAreNewGuestbookEntriesAvailableMessage) {
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            boolean available = user != null && DATABASE.getAreNewGuestbookEntriesAvailable(user.userId);
            connection.sendTCP(new AreNewGuestbookEntriesAvailableResponseMessage(available));
            return;
        }

        if (msg instanceof StoreAreNewGuestbookEntriesAvailableMessage) {
            StoreAreNewGuestbookEntriesAvailableMessage m = (StoreAreNewGuestbookEntriesAvailableMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                return;
            }
            try {
                DATABASE.setAreNewGuestbookEntriesAvailable(user.userId, m.isAvailable());
            } catch (IOException e) {
                log("[Server] Failed to store guestbook availability: " + e.getMessage());
            }
            return;
        }

        if (msg instanceof RequestAvatarDataMessage) {
            RequestAvatarDataMessage m = (RequestAvatarDataMessage) msg;
            int playerId = resolveReadablePlayerId(connection, m.getPlayerID());
            NordDatabase.UserRecord user = DATABASE.findByUserId(playerId);
            String fallbackName = user != null ? user.username : ("Player " + playerId);
            int fallbackLevel = user != null ? user.level : 1;
            AvatarDetails details = DATABASE.loadAvatarDetails(playerId, fallbackName, fallbackLevel);
            log("[Server] Responding avatar data playerId=" + playerId + " bytes=" + safeBytes(details.getAvatarData()).length);
            connection.sendTCP(new RequestAvatarDataResponseMessage(details));
            return;
        }

        if (msg instanceof StoreAvatarDataMessage) {
            StoreAvatarDataMessage m = (StoreAvatarDataMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                return;
            }
            byte[] avatarData = m.getData();
            try {
                DATABASE.storeAvatarData(user.userId, avatarData);
            } catch (IOException e) {
                log("[Server] Failed to store avatar data: " + e.getMessage());
            }
            int villageId = ACTIVE_VILLAGE_BY_CONNECTION.getOrDefault(connection.getID(), 0);
            if (villageId != 0) {
                // Keep remote avatars in-place: harvest/water can trigger avatar data writes, and forcing
                // leave+enter here can make other clients temporarily drop the actor until next movement.
                broadcastToVillage(villageId, new StoreAvatarDataNotificationMessage(villageId, user.userId, safeBytes(avatarData)), null, true, false);
                AvatarDetails refreshed = DATABASE.loadAvatarDetails(user.userId, user.username, user.level);
                // The client ignores self EnterVillage avatar notifications; send direct avatar data response too.
                connection.sendTCP(new RequestAvatarDataResponseMessage(refreshed));
                log("[Server] Refreshed avatar in village playerId=" + user.userId + " bytes=" + safeBytes(refreshed.getAvatarData()).length);
            }
            return;
        }

        if (msg instanceof StoreMountMessage) {
            StoreMountMessage m = (StoreMountMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user != null) {
                try {
                    if (!canStoreCompanion(user, "mount", m.getMountType())) {
                        AvatarDetails refreshed = DATABASE.loadAvatarDetails(user.userId, user.username, user.level);
                        connection.sendTCP(new RequestAvatarDataResponseMessage(refreshed));
                        log("[Server] Rejected unauthorized mount store for userId=" + user.userId +
                            " mountType=" + m.getMountType());
                        return;
                    }
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
                    if (!canStoreCompanion(user, "pet", m.getPetType())) {
                        AvatarDetails refreshed = DATABASE.loadAvatarDetails(user.userId, user.username, user.level);
                        connection.sendTCP(new RequestAvatarDataResponseMessage(refreshed));
                        log("[Server] Rejected unauthorized pet store for userId=" + user.userId +
                            " petType=" + m.getPetType());
                        return;
                    }
                    DATABASE.storePet(user.userId, m.getPetType(), m.getPetName());
                } catch (IOException e) {
                    log("[Server] Failed to store pet: " + e.getMessage());
                }
            }
            return;
        }

        if (msg instanceof StorePlayerInformationMessage) {
            StorePlayerInformationMessage m = (StorePlayerInformationMessage) msg;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                return;
            }
            try {
                DATABASE.storePlayerInformation(
                    user.userId,
                    m.getFirstName(),
                    m.getLastName(),
                    m.getBirthYear(),
                    m.getBirthMonth(),
                    m.getBirthDate(),
                    m.getUrlToPresentationPicture(),
                    m.getCountry(),
                    m.getCity() != null ? normalizeStatusText(m.getCity()) : null,
                    m.getWantsEmail()
                );
                String requestedPassword = safe(m.getPassword());
                if (!requestedPassword.trim().isEmpty()) {
                    DATABASE.updateUserPassword(user.userId, requestedPassword);
                }
            } catch (IOException e) {
                log("[Server] Failed to store player information: " + e.getMessage());
            }
            return;
        }

        if (msg instanceof RequestPlayerInformationMessage) {
            RequestPlayerInformationMessage m = (RequestPlayerInformationMessage) msg;
            int playerId = resolveReadablePlayerId(connection, m.getPlayerID());
            NordDatabase.UserRecord user = DATABASE.findByUserId(playerId);
            NordDatabase.AccountProfileRecord profile = DATABASE.loadAccountProfile(playerId);
            NordDatabase.UserRecord requestingUser = USERS_BY_CONNECTION.get(connection.getID());
            String username = user != null ? user.username : "Player " + playerId;
            int homeVillage = user != null ? user.villageId : 0;
            int level = user != null ? user.level : 1;
            int age = DATABASE.getPlayerAgeYears(playerId);
            byte sex = profile != null ? profile.sex : 0;
            String firstName = profile != null ? safe(profile.firstName) : username;
            String surName = profile != null ? safe(profile.surName) : "";
            String city = profile != null ? safe(profile.city) : "";
            // The legacy client has no dedicated outbound "presentation" write field.
            // Mirror persisted profile text to avoid returning an always-empty presentation.
            String presentation = city;
            String country = resolveCountryLabel(profile);
            String presentationPictureUrl = resolvePlayerPhotoUrl(playerId, profile);
            long memberSince = profile != null ? Math.max(0L, profile.createdAt) : 0L;
            int totalScore = resolvePlayerDirectoryScore(user);
            boolean wantsEmail = profile != null && profile.wantsEmail;
            int requestingPlayerId = requestingUser != null ? requestingUser.userId : 0;
            boolean hasYouBlocked =
                requestingPlayerId > 0 &&
                requestingPlayerId != playerId &&
                DATABASE.isPlayerBlocked(playerId, requestingPlayerId);
            boolean blockedByYou =
                requestingPlayerId > 0 &&
                requestingPlayerId != playerId &&
                DATABASE.isPlayerBlocked(requestingPlayerId, playerId);
            Connection targetConnection = findConnectionByPlayerId(playerId);
            boolean isOnline = targetConnection != null;
            int visitingVillage = 0;
            if (targetConnection != null) {
                visitingVillage = ACTIVE_VILLAGE_BY_CONNECTION.getOrDefault(targetConnection.getID(), homeVillage);
            }
            connection.sendTCP(new RequestPlayerInformationResponseMessage(
                playerId,
                firstName,
                surName,
                username,
                memberSince,
                age,
                presentation,
                presentationPictureUrl,
                totalScore,
                sex,
                homeVillage,
                visitingVillage,
                isOnline,
                country,
                city,
                wantsEmail,
                level,
                resolvePlayerTheme(user),
                hasYouBlocked,
                blockedByYou
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
                NordDatabase.LevelRewardResult result =
                    DATABASE.applyCompletedLevelReward(
                        playerId,
                        m.getLevel(),
                        LEVEL_UP_REWARD_BASE_CREDITS,
                        LEVEL_UP_REWARD_STEP_CREDITS
                    );
                boolean adjustedByOne = false;
                // Some client flows report completed level as (stored level - 1).
                // If no reward was granted and the reported level is exactly one below
                // the previously stored level, retry using +1 so level-up coins are granted.
                if (result.creditsAwarded == 0 && m.getLevel() == result.previousLevel - 1) {
                    NordDatabase.LevelRewardResult adjusted =
                        DATABASE.applyCompletedLevelReward(
                            playerId,
                            m.getLevel() + 1,
                            LEVEL_UP_REWARD_BASE_CREDITS,
                            LEVEL_UP_REWARD_STEP_CREDITS
                        );
                    if (adjusted.creditsAwarded > 0) {
                        result = adjusted;
                        adjustedByOne = true;
                    }
                }
                refreshCachedUserRecord(playerId);
                connection.sendTCP(new RequestSLXCreditsResponseMessage(result.creditsAfter));
                if (result.creditsAwarded > 0) {
                    log("[Server] Level reward granted playerId=" + playerId +
                        " completedLevel=" + m.getLevel() +
                        " adjustedByOne=" + adjustedByOne +
                        " previousLevel=" + result.previousLevel +
                        " newLevel=" + result.newLevel +
                        " creditsAwarded=" + result.creditsAwarded +
                        " creditsAfter=" + result.creditsAfter);
                } else {
                    log("[Server] CompletedLevel processed playerId=" + playerId +
                        " completedLevel=" + m.getLevel() +
                        " adjustedByOne=" + adjustedByOne +
                        " previousLevel=" + result.previousLevel +
                        " newLevel=" + result.newLevel +
                        " creditsAwarded=0");
                }
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
                NordDatabase.SpinStateRecord existing = DATABASE.loadSpinState(user.userId);
                boolean incomingEmpty = isSpinStateEmpty(m);
                boolean existingHasProgress = hasSpinProgress(existing);
                if (incomingEmpty && existingHasProgress) {
                    // Some reconnect/login flows can emit a default wheel snapshot; don't clobber restored DB state.
                    return;
                }
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
                DATABASE.storeWheelResourceState(
                    user.userId,
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
                log("[Server] Failed to persist spin state: " + e.getMessage());
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

    private static void handleGenericByteArrayMessage(Connection connection,
                                                      com.slx.nord.jgnmessages.clientinbound.GenericByteArrayMessage message) {
        if (connection == null || message == null) {
            return;
        }
        if (message.getType() != 0) {
            int seen = UNHANDLED_MESSAGE_COUNTS.merge("generic:type:" + message.getType(), 1, Integer::sum);
            if (seen <= 5) {
                log("[Server] Ignoring unsupported GenericByteArrayMessage type=" + message.getType());
            }
            return;
        }

        byte[] payloadBytes = safeBytes(message.getData());
        if (payloadBytes.length == 0) {
            return;
        }
        if (payloadBytes.length > MAX_GENERIC_BYTEARRAY_BYTES) {
            log("[Server] Ignoring oversized GenericByteArrayMessage payload bytes=" + payloadBytes.length);
            return;
        }

        Object payload;
        try {
            payload = deserializeWhitelistedGenericPayload(payloadBytes);
        } catch (Exception e) {
            log("[Server] Failed to decode GenericByteArrayMessage payload: " + e.getMessage());
            return;
        }
        if (payload == null) {
            return;
        }

        if (payload instanceof BindSessionToClientMessage) {
            BindSessionToClientMessage m = (BindSessionToClientMessage) payload;
            int requestedSessionId = m.getSessionID();
            Integer currentSessionId = SERVER_SESSION_ID_BY_CONNECTION.get(connection.getID());
            if (currentSessionId != null) {
                if (currentSessionId == requestedSessionId) {
                    return;
                }
                log("[Server] Rejected BindSessionToClientMessage on authenticated connection due to mismatched session id");
                return;
            }
            int reboundPlayerId = resolvePlayerIdForServerSession(requestedSessionId);
            if (reboundPlayerId <= 0) {
                log("[Server] Rejected BindSessionToClientMessage due to unknown session id");
                return;
            }
            NordDatabase.UserRecord reboundUser = DATABASE.findByUserId(reboundPlayerId);
            if (reboundUser == null) {
                PLAYER_ID_BY_SERVER_SESSION_ID.remove(requestedSessionId);
                log("[Server] Rejected BindSessionToClientMessage due to missing rebound user for session " + requestedSessionId);
                return;
            }
            Connection existingConnection = findConnectionByPlayerId(reboundUser.userId);
            if (existingConnection != null && existingConnection.getID() != connection.getID()) {
                existingConnection.close();
            }
            USERS_BY_CONNECTION.put(connection.getID(), reboundUser);
            SERVER_SESSION_ID_BY_CONNECTION.put(connection.getID(), requestedSessionId);
            maybePromoteExpiredBuildingCooldowns(reboundUser.villageId);
            return;
        }

        if (payload instanceof AddServerSessionParametersMessage) {
            AddServerSessionParametersMessage m = (AddServerSessionParametersMessage) payload;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                return;
            }
            if (!isValidServerSession(connection, m.getSessionID())) {
                log("[Server] Rejected AddServerSessionParametersMessage from userId=" + user.userId + " due to invalid session id");
                return;
            }
            try {
                DATABASE.storeServerSessionParameters(
                    user.userId,
                    m.getSessionID(),
                    safe(m.getSystem()),
                    safe(m.getJava()),
                    safe(m.getAdapter()),
                    safe(m.getDisplayVendor()),
                    safe(m.getDisplayRenderer()),
                    safe(m.getDisplayDriverVersion()),
                    safe(m.getDisplayApiVersion())
                );
            } catch (IOException e) {
                log("[Server] Failed to store server session parameters for userId=" + user.userId + ": " + e.getMessage());
            }
            return;
        }

        if (payload instanceof RequestTwitterTokensMessage) {
            RequestTwitterTokensMessage m = (RequestTwitterTokensMessage) payload;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                return;
            }
            if (!isValidServerSession(connection, m.getSessionID())) {
                log("[Server] Rejected RequestTwitterTokensMessage from userId=" + user.userId + " due to invalid session id");
                return;
            }
            NordDatabase.TwitterTokensRecord tokens = DATABASE.loadTwitterTokens(user.userId);
            String token = null;
            String tokenSecret = null;
            if (tokens != null) {
                String storedToken = safe(tokens.token).trim();
                String storedSecret = safe(tokens.tokenSecret).trim();
                if (!storedToken.isEmpty() && !storedSecret.isEmpty()) {
                    token = storedToken;
                    tokenSecret = storedSecret;
                }
            }
            sendGenericByteArraySerializable(connection, new TwitterTokensResponseMessage(token, tokenSecret), "TwitterTokensResponseMessage");
            return;
        }

        if (payload instanceof GetBuildingsChecksumMessage) {
            if (!ENABLE_BUILDINGS_CHECKSUM_RESPONSES) {
                return;
            }
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                return;
            }
            GetBuildingsChecksumMessage m = (GetBuildingsChecksumMessage) payload;
            int villageId = resolveVillageId(connection, m.getVillageID());
            if (villageId <= 0) {
                return;
            }
            int syncedVillageId = LAST_BUILDINGS_SYNC_VILLAGE_BY_CONNECTION.getOrDefault(connection.getID(), 0);
            if (syncedVillageId != villageId) {
                log("[Server] Skipped GetBuildingsChecksumMessage for userId=" + user.userId +
                    " villageId=" + villageId +
                    " until RequestBuildings sync is established");
                return;
            }
            long checksum = calculateBuildingsChecksum(villageId);
            sendGenericByteArraySerializable(
                connection,
                new BuildingsChecksumResponseMessage(checksum, villageId),
                "BuildingsChecksumResponseMessage"
            );
            return;
        }

        if (payload instanceof StoreTwitterTokensMessage) {
            StoreTwitterTokensMessage m = (StoreTwitterTokensMessage) payload;
            NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
            if (user == null) {
                return;
            }
            if (!isValidServerSession(connection, m.getSessionID())) {
                log("[Server] Rejected StoreTwitterTokensMessage from userId=" + user.userId + " due to invalid session id");
                return;
            }
            try {
                DATABASE.storeTwitterTokens(user.userId, safe(m.getToken()), safe(m.getTokenSecret()), m.getSessionID());
            } catch (IOException e) {
                log("[Server] Failed to store twitter tokens for userId=" + user.userId + ": " + e.getMessage());
            }
            return;
        }

        if (payload instanceof BlockPlayerMessage) {
            BlockPlayerMessage m = (BlockPlayerMessage) payload;
            int playerId = resolvePlayerId(connection, m.getOwnPlayerId());
            if (playerId == 0) {
                return;
            }
            int blockedPlayerId = m.getOtherPlayerId();
            if (blockedPlayerId <= 0 || blockedPlayerId == playerId) {
                return;
            }
            NordDatabase.UserRecord blockedUser = DATABASE.findByUserId(blockedPlayerId);
            if (blockedUser == null) {
                return;
            }
            if (isModeratorOrAdmin(blockedPlayerId)) {
                sendGenericByteArraySerializable(
                    connection,
                    new CouldNotBanUserMessage(blockedPlayerId, COULD_NOT_BAN_REASON_MODERATOR_OR_ADMIN),
                    "CouldNotBanUserMessage"
                );
                sendBlockedPlayersSnapshot(connection, playerId);
                log("[Server] Rejecting block attempt userId=" + playerId +
                    " targetId=" + blockedPlayerId + " reason=moderator_or_admin");
                return;
            }
            try {
                DATABASE.addBlockedPlayer(playerId, blockedPlayerId);
            } catch (IOException e) {
                log("[Server] Failed to block playerId=" + blockedPlayerId + " for userId=" + playerId + ": " + e.getMessage());
            }
            sendBlockedPlayersSnapshot(connection, playerId);
            return;
        }

        if (payload instanceof UnblockPlayerMessage) {
            UnblockPlayerMessage m = (UnblockPlayerMessage) payload;
            int playerId = resolvePlayerId(connection, m.getOwnPlayerId());
            if (playerId == 0) {
                return;
            }
            int blockedPlayerId = m.getOtherPlayerId();
            if (blockedPlayerId <= 0 || blockedPlayerId == playerId) {
                return;
            }
            try {
                DATABASE.removeBlockedPlayer(playerId, blockedPlayerId);
            } catch (IOException e) {
                log("[Server] Failed to unblock playerId=" + blockedPlayerId + " for userId=" + playerId + ": " + e.getMessage());
            }
            sendBlockedPlayersSnapshot(connection, playerId);
            return;
        }

        String genericType = payload.getClass().getName();
        int seen = UNHANDLED_MESSAGE_COUNTS.merge("generic:payload:" + genericType, 1, Integer::sum);
        if (seen <= 5) {
            log("[Server] Unhandled generic payload (" + seen + "): " + genericType);
        }
    }

    private static Object deserializeWhitelistedGenericPayload(byte[] payloadBytes) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream input = new ByteArrayInputStream(payloadBytes);
             WhitelistObjectInputStream objectInput =
                 new WhitelistObjectInputStream(input, ALLOWED_INBOUND_GENERIC_PAYLOAD_CLASSES)) {
            Object payload = objectInput.readObject();
            if (payload == null) {
                return null;
            }
            String payloadClassName = payload.getClass().getName();
            if (!ALLOWED_INBOUND_GENERIC_PAYLOAD_CLASSES.contains(payloadClassName)) {
                throw new InvalidClassException("Unexpected generic payload class", payloadClassName);
            }
            return payload;
        }
    }

    private static boolean isAllowedSerializedClassName(String className, Set<String> allowedClassNames) {
        if (className == null || className.isEmpty()) {
            return false;
        }
        return allowedClassNames.contains(className) ||
               "java.lang.String".equals(className);
    }

    private static boolean isValidServerSession(Connection connection, int providedSessionId) {
        if (connection == null || providedSessionId <= 0) {
            return false;
        }
        Integer expectedSessionId = SERVER_SESSION_ID_BY_CONNECTION.get(connection.getID());
        if (expectedSessionId == null) {
            return false;
        }
        return expectedSessionId == providedSessionId;
    }

    private static int resolvePlayerIdForServerSession(int sessionId) {
        if (sessionId <= 0) {
            return 0;
        }
        Integer playerId = PLAYER_ID_BY_SERVER_SESSION_ID.get(sessionId);
        return playerId == null ? 0 : Math.max(0, playerId);
    }

    private static void rememberServerSessionForPlayer(int sessionId, int playerId) {
        if (sessionId <= 0 || playerId <= 0) {
            return;
        }
        for (Map.Entry<Integer, Integer> entry : PLAYER_ID_BY_SERVER_SESSION_ID.entrySet()) {
            Integer activePlayerId = entry.getValue();
            if (activePlayerId != null && activePlayerId == playerId && entry.getKey() != sessionId) {
                PLAYER_ID_BY_SERVER_SESSION_ID.remove(entry.getKey());
            }
        }
        PLAYER_ID_BY_SERVER_SESSION_ID.put(sessionId, playerId);
    }

    private static String resolveRequiredClientVersion() {
        String fromProperty = safe(System.getProperty("nord.client.version.required")).trim();
        if (!fromProperty.isEmpty()) {
            return fromProperty;
        }
        return safe(System.getenv("NORD_CLIENT_VERSION_REQUIRED")).trim();
    }

    private static boolean isAcceptedClientVersion(String providedVersion, String requiredVersion) {
        String required = safe(requiredVersion).trim();
        if (required.isEmpty()) {
            return true;
        }
        return required.equals(safe(providedVersion).trim());
    }

    private static void sendPlayerBlacklisted(Connection connection, NordDatabase.LoginBlacklistRecord blacklist) {
        if (connection == null || blacklist == null) {
            return;
        }
        sendGenericByteArraySerializable(
            connection,
            new PlayerBlacklistedMessage(safe(blacklist.message), blacklist.blockedUntil),
            "PlayerBlacklistedMessage"
        );
    }

    private static void sendGenericByteArraySerializable(Connection connection, Serializable payload, String label) {
        if (connection == null || payload == null) {
            return;
        }
        try {
            connection.sendTCP(
                com.slx.nord.jgnmessages.clientinbound.GenericByteArrayMessage.createSerializedByteArrayMessage(payload)
            );
        } catch (Exception e) {
            log("[Server] Failed to send " + safe(label) + ": " + e.getMessage());
        }
    }

    private static synchronized void joinVillage(Connection connection, int villageId) {
        villageId = sanitizeRequestedVillageId(connection, villageId, "joinVillage");
        if (villageId <= 0) {
            return;
        }

        NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connection.getID());
        if (user == null) {
            return;
        }

        int oldVillageId = sanitizeActiveVillageId(
            connection,
            ACTIVE_VILLAGE_BY_CONNECTION.getOrDefault(connection.getID(), 0),
            "joinVillage"
        );
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
        AvatarDetails enteringAvatar = DATABASE.loadAvatarDetails(user.userId, user.username, user.level);
        entering.add(enteringAvatar);
        log("[Server] EnterVillage avatar playerId=" + user.userId + " bytes=" + safeBytes(enteringAvatar.getAvatarData()).length);
        broadcastToVillage(villageId, new EnterVillageWithAvatarNotificationMessage(villageId, entering), connection, false, false);
        AvatarPosition enteringPosition = getOrSeedAvatarPosition(villageId, user.userId);
        broadcastAvatarPositionToVillageWithRetry(villageId, user.userId, enteringPosition, connection, false);
    }

    private static synchronized void leaveVillage(Connection connection, boolean notifyOthers) {
        int connectionId = connection.getID();
        int villageId = sanitizeActiveVillageId(
            connection,
            ACTIVE_VILLAGE_BY_CONNECTION.getOrDefault(connectionId, 0),
            "leaveVillage"
        );
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
            AvatarPosition position = getOrSeedAvatarPosition(villageId, playerId);
            sendAvatarPositionWithRetry(connection, villageId, playerId, position);
        }
    }

    private static void rebroadcastAvatarPositionIfKnown(int villageId, int playerId, Connection excludeConnection) {
        if (villageId <= 0 || playerId <= 0) {
            return;
        }
        AvatarPosition position = AVATAR_POSITIONS_BY_PLAYER.get(playerId);
        if (position == null || position.villageId != villageId) {
            return;
        }
        // Interaction updates (harvest/water/consume/etc.) do not always include movement, so push the
        // last known position to keep remote clients from hiding this avatar until manual movement occurs.
        broadcastToVillage(
            villageId,
            new MoveAvatarNotificationMessage(
                villageId,
                playerId,
                position.posX,
                position.posZ,
                position.rotation
            ),
            excludeConnection,
            false,
            false
        );
    }

    private static AvatarPosition getOrSeedAvatarPosition(int villageId, int playerId) {
        if (villageId <= 0 || playerId <= 0) {
            return new AvatarPosition(villageId, DEFAULT_AVATAR_POS_X, DEFAULT_AVATAR_POS_Z, DEFAULT_AVATAR_ROTATION);
        }
        AvatarPosition known = AVATAR_POSITIONS_BY_PLAYER.get(playerId);
        if (known != null && known.villageId == villageId) {
            return known;
        }
        AvatarPosition seeded = new AvatarPosition(villageId, DEFAULT_AVATAR_POS_X, DEFAULT_AVATAR_POS_Z, DEFAULT_AVATAR_ROTATION);
        AVATAR_POSITIONS_BY_PLAYER.put(playerId, seeded);
        return seeded;
    }

    private static void broadcastAvatarPositionToVillageWithRetry(int villageId,
                                                                   int playerId,
                                                                   AvatarPosition position,
                                                                   Connection excludeConnection,
                                                                   boolean includeSender) {
        if (villageId <= 0 || playerId <= 0 || position == null) {
            return;
        }
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
            sendAvatarPositionWithRetry(target, villageId, playerId, position);
        }
    }

    private static void sendAvatarPositionWithRetry(Connection connection,
                                                    int villageId,
                                                    int playerId,
                                                    AvatarPosition position) {
        if (connection == null || villageId <= 0 || playerId <= 0 || position == null) {
            return;
        }
        sendAvatarPositionToConnection(connection, villageId, playerId, position);
        for (long delayMs : INITIAL_AVATAR_MOVE_RETRY_DELAYS_MS) {
            if (delayMs <= 0L) {
                continue;
            }
            int connectionId = connection.getID();
            AVATAR_POSITION_REPLAY_EXECUTOR.schedule(() -> {
                Connection current = CONNECTIONS_BY_ID.get(connectionId);
                if (current == null || current.getID() != connectionId) {
                    return;
                }
                if (!isConnectionInVillage(current, villageId)) {
                    return;
                }
                sendAvatarPositionToConnection(current, villageId, playerId, position);
            }, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    private static void sendAvatarPositionToConnection(Connection connection,
                                                       int villageId,
                                                       int playerId,
                                                       AvatarPosition position) {
        connection.sendTCP(new MoveAvatarNotificationMessage(
            villageId,
            playerId,
            position.posX,
            position.posZ,
            position.rotation
        ));
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

    private static void broadcastToOnline(Object payload, Connection excludeConnection, boolean includeSender) {
        for (Map.Entry<Integer, NordDatabase.UserRecord> entry : USERS_BY_CONNECTION.entrySet()) {
            Connection target = CONNECTIONS_BY_ID.get(entry.getKey());
            if (target == null) {
                continue;
            }
            if (excludeConnection != null && target.getID() == excludeConnection.getID() && !includeSender) {
                continue;
            }
            target.sendTCP(payload);
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
        SERVER_SESSION_ID_BY_CONNECTION.remove(connection.getID());
        LAST_BUILDINGS_SYNC_VILLAGE_BY_CONNECTION.remove(connection.getID());
    }

    private static void removeFromVillageConnectionMap(int villageId, int connectionId) {
        Map<Integer, Connection> villageConnections = VILLAGE_CONNECTIONS.get(villageId);
        if (villageConnections == null) {
            return;
        }
        villageConnections.remove(connectionId);
        if (villageConnections.isEmpty()) {
            VILLAGE_CONNECTIONS.remove(villageId);
            LAST_RESOURCE_RESPAWN_SCAN_BY_VILLAGE.remove(villageId);
        }
    }

    private static boolean isInvalidVillageIdSentinel(int villageId) {
        return villageId == INVALID_VILLAGE_ID_SENTINEL;
    }

    private static int sanitizeRequestedVillageId(Connection connection, int requestedVillageId, String context) {
        if (!isInvalidVillageIdSentinel(requestedVillageId)) {
            return requestedVillageId;
        }
        int connectionId = connection != null ? connection.getID() : 0;
        NordDatabase.UserRecord user = connection != null ? USERS_BY_CONNECTION.get(connectionId) : null;
        log("[Server] Ignored invalid requested villageId=" + requestedVillageId +
            " in " + safe(context) +
            " connectionId=" + connectionId +
            " userId=" + (user != null ? user.userId : 0));
        return 0;
    }

    private static int sanitizeActiveVillageId(Connection connection, int activeVillageId, String context) {
        if (!isInvalidVillageIdSentinel(activeVillageId)) {
            return activeVillageId;
        }
        if (connection == null) {
            return 0;
        }
        int connectionId = connection.getID();
        NordDatabase.UserRecord user = USERS_BY_CONNECTION.get(connectionId);
        log("[Server] Dropped invalid active villageId=" + activeVillageId +
            " in " + safe(context) +
            " connectionId=" + connectionId +
            " userId=" + (user != null ? user.userId : 0));
        ACTIVE_VILLAGE_BY_CONNECTION.remove(connectionId);
        removeFromVillageConnectionMap(activeVillageId, connectionId);
        return 0;
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
        int sanitizedRequestedVillageId = sanitizeRequestedVillageId(connection, requestedVillageId, "resolveVillageId");
        int resolvedVillageId;
        if (sanitizedRequestedVillageId != 0) {
            resolvedVillageId = sanitizedRequestedVillageId;
        } else {
            int currentVillage = sanitizeActiveVillageId(
                connection,
                ACTIVE_VILLAGE_BY_CONNECTION.getOrDefault(connection.getID(), 0),
                "resolveVillageId"
            );
            if (currentVillage != 0) {
                resolvedVillageId = currentVillage;
            } else {
                NordDatabase.UserRecord currentUser = USERS_BY_CONNECTION.get(connection.getID());
                resolvedVillageId = currentUser != null ? currentUser.villageId : 0;
            }
        }
        maybePromoteExpiredBuildingCooldowns(resolvedVillageId);
        return resolvedVillageId;
    }

    private static int resolveLiveVillageId(Connection connection, int requestedVillageId) {
        NordDatabase.UserRecord currentUser = USERS_BY_CONNECTION.get(connection.getID());
        if (currentUser == null) {
            return 0;
        }
        int sanitizedRequestedVillageId = sanitizeRequestedVillageId(connection, requestedVillageId, "resolveLiveVillageId");
        int currentVillage = sanitizeActiveVillageId(
            connection,
            ACTIVE_VILLAGE_BY_CONNECTION.getOrDefault(connection.getID(), 0),
            "resolveLiveVillageId"
        );

        // Explicit village ids in write messages must match the active live village stream.
        // Otherwise stale/out-of-order packets can mutate buildings in the wrong village.
        if (sanitizedRequestedVillageId != 0) {
            if (currentVillage != 0 && sanitizedRequestedVillageId != currentVillage) {
                log("[Server] Rejected mismatched live village write from userId=" + currentUser.userId +
                    " requestedVillageId=" + sanitizedRequestedVillageId +
                    " activeVillageId=" + currentVillage);
                return 0;
            }
            if (currentVillage == 0 && sanitizedRequestedVillageId != currentUser.villageId) {
                log("[Server] Rejected cross-village write access from userId=" + currentUser.userId +
                    " requestedVillageId=" + sanitizedRequestedVillageId +
                    " homeVillageId=" + currentUser.villageId);
                return 0;
            }
            maybePromoteExpiredBuildingCooldowns(sanitizedRequestedVillageId);
            return sanitizedRequestedVillageId;
        }

        if (currentVillage != 0) {
            maybePromoteExpiredBuildingCooldowns(currentVillage);
            return currentVillage;
        }
        maybePromoteExpiredBuildingCooldowns(currentUser.villageId);
        return currentUser.villageId;
    }

    private static void maybePromoteExpiredBuildingCooldowns(int villageId) {
        if (villageId <= 0) {
            return;
        }
        if (!isVillageOwnerOnline(villageId)) {
            return;
        }
        long now = System.currentTimeMillis();
        long previous = LAST_RESOURCE_RESPAWN_SCAN_BY_VILLAGE.getOrDefault(villageId, 0L);
        if (now - previous < RESOURCE_RESPAWN_SCAN_INTERVAL_MS) {
            return;
        }
        LAST_RESOURCE_RESPAWN_SCAN_BY_VILLAGE.put(villageId, now);

        ArrayList<Building> promoted;
        try {
            promoted = DATABASE.promoteExpiredBuildingCooldowns(
                villageId,
                now,
                BUILDING_REQUIRED_INGREDIENT_COUNT_BY_TYPE
            );
        } catch (IOException e) {
            log("[Server] Failed to promote expired building cooldowns in villageId=" + villageId + ": " + e.getMessage());
            return;
        }
        if (promoted.isEmpty()) {
            return;
        }

        log("[Server] Promoted " + promoted.size() + " expired building cooldown(s) in villageId=" + villageId);
        for (Building building : promoted) {
            if (building == null) {
                continue;
            }
            broadcastToVillage(
                villageId,
                new UpdateBuildingNotificationMessage(
                    villageId,
                    building.getBuildingID(),
                    safeBytes(building.getParameters()),
                    building.isConsumed(),
                    building.isPlacedOnMap(),
                    building.getTileX(),
                    building.getTileZ(),
                    building.getRotation(),
                    true,
                    building.getDelayStart(),
                    building.getDelayEnd(),
                    building.getIngredients()
                ),
                null,
                true,
                false
            );
        }
    }

    private static boolean isVillageOwnerOnline(int villageId) {
        if (villageId <= 0) {
            return false;
        }
        NordDatabase.UserRecord owner = DATABASE.findByVillageId(villageId);
        if (owner == null || owner.userId <= 0) {
            return false;
        }
        return findConnectionByPlayerId(owner.userId) != null;
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
            NordDatabase.AccountProfileRecord profile = DATABASE.loadAccountProfile(user.userId);
            data.names.add(user.username);
            data.villageIDs.add(user.villageId);
            data.userIDs.add(user.userId);
            data.ages.add(DATABASE.getPlayerAgeYears(user.userId));
            data.sexes.add(DATABASE.getPlayerSex(user.userId));
            data.countries.add(resolveCountryLabel(profile));
            data.cities.add(profile != null ? safe(profile.city) : "");
            data.scores.add(resolvePlayerDirectoryScore(user));
            data.onlines.add(true);
            data.photoURLs.add(resolvePlayerPhotoUrl(user.userId, profile));
            data.villageNames.add(resolveVillageName(user.villageId, user.villageName));
            data.visitors.add(villageVisitorCount(user.villageId));
            data.themes.add(resolvePlayerTheme(user));
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

    private static int resolvePlayerDirectoryScore(NordDatabase.UserRecord user) {
        if (user == null) {
            return 0;
        }
        return Math.max(0, user.slxCredits);
    }

    private static String resolveCountryLabel(NordDatabase.AccountProfileRecord profile) {
        if (profile == null || profile.countryId <= 0) {
            return "";
        }
        return Integer.toString(profile.countryId);
    }

    private static String resolvePlayerPhotoUrl(int playerId, NordDatabase.AccountProfileRecord profile) {
        if (playerId <= 0) {
            return "";
        }
        String profilePictureUrl = profile != null ? safe(profile.presentationPictureUrl).trim() : "";
        if (!profilePictureUrl.isEmpty()) {
            return profilePictureUrl;
        }
        int latestImageId = DATABASE.loadLatestUploadedPictureId(playerId);
        if (latestImageId <= 0) {
            return "";
        }
        return resolvePlayerImagesPrefix() + latestImageId + ".jpg";
    }

    private static byte resolvePlayerTheme(int playerId) {
        NordDatabase.UserRecord user = DATABASE.findByUserId(playerId);
        return resolvePlayerTheme(user);
    }

    private static byte resolvePlayerTheme(NordDatabase.UserRecord user) {
        if (user == null) {
            return 0;
        }
        return resolveVillageTheme(user.villageId);
    }

    private static byte resolveVillageTheme(int villageId) {
        if (villageId <= 0) {
            return 0;
        }
        return DATABASE.loadVillageTheme(villageId);
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
        if (existing == null) {
            log("[Server] Ignored building delta for missing row villageId=" + villageId + " buildingId=" + buildingId);
            return;
        }
        byte[] nextParameters = parameters != null ? safeBytes(parameters) : safeBytes(existing.getParameters());
        boolean nextConsumed = consumed != null ? consumed : existing.isConsumed();
        boolean nextPlacedOnMap = placedOnMap != null ? placedOnMap : existing.isPlacedOnMap();
        byte nextTileX = existing.getTileX();
        byte nextTileZ = existing.getTileZ();
        byte nextRotation = existing.getRotation();
        boolean nextReady = ready != null ? ready : existing.isReady();
        long now = System.currentTimeMillis();
        long previousDelayStart = existing.getDelayStart();
        long previousDelayEnd = existing.getDelayEnd();
        long nextIngredients = ingredients != null ? ingredients : existing.getIngredients();
        long nextDelayStart;
        long nextDelayEnd;
        if (writeDelayStart) {
            // Client writes delayEnd as (delayStart + offset), and offset can legitimately be zero.
            long normalizedOffset = Math.max(0L, delayEndOffset);
            nextDelayStart = now;
            if (Long.MAX_VALUE - now < normalizedOffset) {
                nextDelayEnd = Long.MAX_VALUE;
            } else {
                nextDelayEnd = now + normalizedOffset;
            }
        } else {
            nextDelayStart = previousDelayStart;
            nextDelayEnd = previousDelayEnd;
            // Harvest without a restarted delay window means "await ingredients"; clear stale cooldowns
            // so the periodic respawn promotion does not immediately flip the node back to ready.
            int requiredIngredientCount =
                Math.max(0, BUILDING_REQUIRED_INGREDIENT_COUNT_BY_TYPE.getOrDefault(existing.getBuildingType(), 0));
            if (Boolean.FALSE.equals(ready) && nextIngredients == 0L && requiredIngredientCount > 0) {
                nextDelayStart = 0L;
                nextDelayEnd = 0L;
            } else if (Boolean.FALSE.equals(ready) &&
                       requiredIngredientCount == 0 &&
                       RESOURCE_HARVEST_BUILDING_TYPE_IDS.contains(existing.getBuildingType())) {
                boolean hasActiveCooldown = previousDelayEnd > now && previousDelayEnd > previousDelayStart;
                if (!hasActiveCooldown) {
                    long fallbackOffset = Math.max(0L, delayEndOffset);
                    if (fallbackOffset <= 0L && previousDelayEnd > previousDelayStart) {
                        fallbackOffset = previousDelayEnd - previousDelayStart;
                    }
                    if (fallbackOffset <= 0L) {
                        fallbackOffset = Math.max(0L, WILD_RESOURCE_RESPAWN_DELAY_MS);
                    }
                    if (fallbackOffset > 0L) {
                        nextDelayStart = now;
                        nextDelayEnd = safeAddMillis(now, fallbackOffset);
                    }
                }
            }
        }

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

    private static class GuestbookResponseLists {
        final ArrayList<Integer> postIds = new ArrayList<>();
        final ArrayList<String> posterNames = new ArrayList<>();
        final ArrayList<String> postTexts = new ArrayList<>();
        final ArrayList<Integer> posterAges = new ArrayList<>();
        final ArrayList<Byte> posterSexes = new ArrayList<>();
        final ArrayList<Integer> posterUserIds = new ArrayList<>();
        final ArrayList<String> posterPhotoURLs = new ArrayList<>();
        final ArrayList<Long> postTimes = new ArrayList<>();
        final ArrayList<Boolean> posterIsOnline = new ArrayList<>();
        final ArrayList<Boolean> privates = new ArrayList<>();
        final ArrayList<Integer> levels = new ArrayList<>();
        final ArrayList<String> types = new ArrayList<>();
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

        NordDatabase.SpinStateRecord spinState = DATABASE.loadSpinState(user.userId);
        NordDatabase.WheelResourceStateRecord wheelState = DATABASE.loadWheelResourceState(user.userId);
        byte[] loginData = spinState != null
            ? buildWheelResourceLoginPayload(spinState)
            : buildWheelResourceLoginPayload(wheelState);
        int restoredSpins = spinState != null ? Math.max(0, spinState.wins) : 0;
        if (spinState != null) {
            log("[Server] Restored spin state for userId=" + user.userId +
                " wins=" + spinState.wins +
                " updatedAt=" + spinState.updatedAt);
        } else if (wheelState != null) {
            log("[Server] Restored wheel resource state for userId=" + user.userId +
                " updatedAt=" + wheelState.updatedAt);
        }

        int sessionId = (int) (System.currentTimeMillis() & 0x7fffffff);
        int accountAge = DATABASE.getAccountAgeDays(user.userId);
        int playerAge = DATABASE.getPlayerAgeYears(user.userId);
        int referralId = DATABASE.getReferralId(user.userId);
        SERVER_SESSION_ID_BY_CONNECTION.put(connection.getID(), sessionId);
        rememberServerSessionForPlayer(sessionId, user.userId);
        connection.sendTCP(new LoginResponseMessage(
            0,
            sessionId,
            user.userId,
            villageIDs,
            villageNames,
            loginData,
            user.level,
            restoredSpins,
            accountAge,
            playerAge,
            true,
            referralId
        ));
    }

    private static boolean isSpinStateEmpty(StoreSpinMessage state) {
        if (state == null) {
            return true;
        }
        return state.getWins() == 0 &&
               !state.isWon() &&
               !state.isLit1() &&
               !state.isLit2() &&
               !state.isLit3() &&
               !state.isLit4() &&
               !state.isLit5() &&
               !state.isLit6() &&
               !state.isLit7() &&
               !state.isLit8() &&
               state.getRes1() == 0 &&
               state.getRes2() == 0 &&
               state.getRes3() == 0 &&
               state.getRes4() == 0 &&
               state.getRes5() == 0 &&
               state.getRes6() == 0 &&
               state.getRes7() == 0 &&
               state.getRes8() == 0;
    }

    private static boolean hasSpinProgress(NordDatabase.SpinStateRecord state) {
        if (state == null) {
            return false;
        }
        return state.wins != 0 ||
               state.won ||
               state.lit1 ||
               state.lit2 ||
               state.lit3 ||
               state.lit4 ||
               state.lit5 ||
               state.lit6 ||
               state.lit7 ||
               state.lit8 ||
               state.res1 != 0 ||
               state.res2 != 0 ||
               state.res3 != 0 ||
               state.res4 != 0 ||
               state.res5 != 0 ||
               state.res6 != 0 ||
               state.res7 != 0 ||
               state.res8 != 0;
    }

    private static byte[] buildWheelResourceLoginPayload(NordDatabase.SpinStateRecord spinState) {
        if (spinState == null) {
            return new byte[0];
        }
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(32);
            DataOutputStream data = new DataOutputStream(out);
            data.writeShort(1); // payload version
            data.writeBoolean(spinState.lit1);
            data.writeBoolean(spinState.lit2);
            data.writeBoolean(spinState.lit3);
            data.writeBoolean(spinState.lit4);
            data.writeBoolean(spinState.lit5);
            data.writeBoolean(spinState.lit6);
            data.writeBoolean(spinState.lit7);
            data.writeBoolean(spinState.lit8);
            data.writeShort(spinState.res1);
            data.writeShort(spinState.res2);
            data.writeShort(spinState.res3);
            data.writeShort(spinState.res4);
            data.writeShort(spinState.res5);
            data.writeShort(spinState.res6);
            data.writeShort(spinState.res7);
            data.writeShort(spinState.res8);
            data.flush();
            return out.toByteArray();
        } catch (IOException e) {
            return new byte[0];
        }
    }

    private static byte[] buildWheelResourceLoginPayload(NordDatabase.WheelResourceStateRecord wheelState) {
        if (wheelState == null) {
            return new byte[0];
        }
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(32);
            DataOutputStream data = new DataOutputStream(out);
            data.writeShort(1); // payload version
            data.writeBoolean(wheelState.lit1);
            data.writeBoolean(wheelState.lit2);
            data.writeBoolean(wheelState.lit3);
            data.writeBoolean(wheelState.lit4);
            data.writeBoolean(wheelState.lit5);
            data.writeBoolean(wheelState.lit6);
            data.writeBoolean(wheelState.lit7);
            data.writeBoolean(wheelState.lit8);
            data.writeShort(wheelState.res1);
            data.writeShort(wheelState.res2);
            data.writeShort(wheelState.res3);
            data.writeShort(wheelState.res4);
            data.writeShort(wheelState.res5);
            data.writeShort(wheelState.res6);
            data.writeShort(wheelState.res7);
            data.writeShort(wheelState.res8);
            data.flush();
            return out.toByteArray();
        } catch (IOException e) {
            return new byte[0];
        }
    }

    private static void sendBlockedPlayersSnapshot(Connection connection, int playerId) {
        ArrayList<String> names = new ArrayList<>();
        ArrayList<Integer> villageIds = new ArrayList<>();
        ArrayList<Integer> userIds = new ArrayList<>();
        ArrayList<Integer> ages = new ArrayList<>();
        ArrayList<Byte> sexes = new ArrayList<>();
        ArrayList<String> countries = new ArrayList<>();
        ArrayList<String> cities = new ArrayList<>();
        ArrayList<Boolean> onlines = new ArrayList<>();
        ArrayList<String> photoUrls = new ArrayList<>();
        ArrayList<String> villageNames = new ArrayList<>();
        ArrayList<Integer> visitors = new ArrayList<>();
        ArrayList<Byte> themes = new ArrayList<>();
        ArrayList<Integer> levels = new ArrayList<>();

        for (Integer blockedPlayerId : DATABASE.loadBlockedPlayerIds(playerId)) {
            if (blockedPlayerId == null || blockedPlayerId <= 0) {
                continue;
            }
            NordDatabase.UserRecord blockedUser = DATABASE.findByUserId(blockedPlayerId);
            if (blockedUser == null) {
                continue;
            }
            NordDatabase.AccountProfileRecord profile = DATABASE.loadAccountProfile(blockedUser.userId);
            Connection blockedConnection = findConnectionByPlayerId(blockedUser.userId);

            names.add(blockedUser.username);
            villageIds.add(blockedUser.villageId);
            userIds.add(blockedUser.userId);
            ages.add(DATABASE.getPlayerAgeYears(blockedUser.userId));
            sexes.add(DATABASE.getPlayerSex(blockedUser.userId));
            countries.add(resolveCountryLabel(profile));
            cities.add(profile != null ? safe(profile.city) : "");
            onlines.add(blockedConnection != null);
            photoUrls.add(resolvePlayerPhotoUrl(blockedUser.userId, profile));
            villageNames.add(resolveVillageName(blockedUser.villageId, blockedUser.villageName));
            visitors.add(villageVisitorCount(blockedUser.villageId));
            themes.add(resolvePlayerTheme(blockedUser));
            levels.add(blockedUser.level);
        }

        connection.sendTCP(new RequestBlockedPlayersResponseMessage(
            playerId,
            names,
            villageIds,
            userIds,
            ages,
            sexes,
            countries,
            cities,
            onlines,
            photoUrls,
            villageNames,
            visitors,
            themes,
            levels
        ));
    }

    private static boolean isModeratorOrAdmin(int playerId) {
        if (playerId <= 0) {
            return false;
        }
        ArrayList<NordDatabase.AchievementRecord> achievements = DATABASE.loadAchievements(playerId);
        for (NordDatabase.AchievementRecord achievement : achievements) {
            if (achievement == null || achievement.value <= 0) {
                continue;
            }
            if (achievement.category != ACHIEVEMENT_ROLE_CATEGORY) {
                continue;
            }
            if (achievement.type == ACHIEVEMENT_ADMIN_TYPE || achievement.type == ACHIEVEMENT_MODERATOR_TYPE) {
                return true;
            }
        }
        return false;
    }

    private static boolean tryHandleSlashCommand(Connection connection, NordDatabase.UserRecord user, String rawText) {
        if (connection == null || user == null) {
            return false;
        }
        String messageText = safe(rawText).trim();
        if (!messageText.startsWith("/") || messageText.length() <= 1) {
            return false;
        }

        String withoutSlash = messageText.substring(1).trim();
        if (withoutSlash.isEmpty()) {
            return false;
        }

        String[] parts = withoutSlash.split("\\s+");
        String command = safe(parts[0]).toLowerCase(Locale.ROOT);
        if (!SUPPORTED_CHAT_COMMANDS.contains(command)) {
            return false;
        }

        String requiredRole = DATABASE.getChatCommandRequiredRole(command);
        ChatCommandAccessLevel requiredAccessLevel = parseChatCommandAccessLevel(requiredRole);
        if (!hasChatCommandAccess(user.userId, requiredAccessLevel)) {
            sendServerDirectMessage(
                connection,
                "Permission denied for /" + command + ". Required role: " + describeChatCommandAccessLevel(requiredAccessLevel) + "."
            );
            return true;
        }

        if (parts.length < 2) {
            sendServerDirectMessage(connection, "Usage: /" + command + " <value>");
            return true;
        }

        int value;
        try {
            value = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            sendServerDirectMessage(connection, "Invalid value. Usage: /" + command + " <integer>");
            return true;
        }

        try {
            if (CHAT_COMMAND_SET_COINS.equals(command)) {
                int updated = DATABASE.setSLXCredits(user.userId, value);
                refreshCachedUserRecord(user.userId);
                sendServerDirectMessage(connection, "Your coins are now " + updated + ".");
                return true;
            }
            if (CHAT_COMMAND_SET_LEVEL.equals(command)) {
                int updated = DATABASE.setUserLevel(user.userId, value);
                refreshCachedUserRecord(user.userId);
                sendServerDirectMessage(connection, "Your level is now " + updated + ".");
                return true;
            }
        } catch (IOException e) {
            sendServerDirectMessage(connection, "Failed to execute /" + command + ": " + e.getMessage());
            return true;
        }

        return false;
    }

    private static ChatCommandAccessLevel parseChatCommandAccessLevel(String requiredRole) {
        String normalized = safe(requiredRole).trim().toLowerCase(Locale.ROOT);
        if (CHAT_COMMAND_PERMISSION_EVERYONE.equals(normalized)) {
            return ChatCommandAccessLevel.EVERYONE;
        }
        if (CHAT_COMMAND_PERMISSION_VIP.equals(normalized)) {
            return ChatCommandAccessLevel.VIP;
        }
        return ChatCommandAccessLevel.MOD;
    }

    private static boolean hasChatCommandAccess(int playerId, ChatCommandAccessLevel requiredAccessLevel) {
        if (requiredAccessLevel == ChatCommandAccessLevel.EVERYONE) {
            return true;
        }
        if (playerId <= 0) {
            return false;
        }

        boolean hasVip = false;
        boolean hasModerator = false;
        boolean hasAdmin = false;
        ArrayList<NordDatabase.AchievementRecord> achievements = DATABASE.loadAchievements(playerId);
        for (NordDatabase.AchievementRecord achievement : achievements) {
            if (achievement == null || achievement.value <= 0 || achievement.category != ACHIEVEMENT_ROLE_CATEGORY) {
                continue;
            }
            if (achievement.type == ACHIEVEMENT_VIP_TYPE) {
                hasVip = true;
            } else if (achievement.type == ACHIEVEMENT_MODERATOR_TYPE) {
                hasModerator = true;
            } else if (achievement.type == ACHIEVEMENT_ADMIN_TYPE) {
                hasAdmin = true;
            }
        }

        if (requiredAccessLevel == ChatCommandAccessLevel.VIP) {
            return hasVip || hasModerator || hasAdmin;
        }
        return hasModerator || hasAdmin;
    }

    private static String describeChatCommandAccessLevel(ChatCommandAccessLevel requiredAccessLevel) {
        if (requiredAccessLevel == ChatCommandAccessLevel.EVERYONE) {
            return "everyone";
        }
        if (requiredAccessLevel == ChatCommandAccessLevel.VIP) {
            return "vip";
        }
        return "mod";
    }

    private static void sendServerDirectMessage(Connection connection, String message) {
        if (connection == null) {
            return;
        }
        connection.sendTCP(new SendTextToPlayerNotificationMessage(0, "Server", safe(message)));
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
        if (playerId <= 0) {
            connection.sendTCP(new RequestIMListResponseMessage(playerId, list));
            return;
        }
        ArrayList<Integer> friendPlayerIds = DATABASE.loadIMListPlayerIds(playerId);
        for (Integer friendPlayerId : friendPlayerIds) {
            if (friendPlayerId == null || friendPlayerId <= 0) {
                continue;
            }
            NordDatabase.UserRecord user = DATABASE.findByUserId(friendPlayerId);
            if (user == null) {
                continue;
            }
            Connection activeConnection = findConnectionByPlayerId(user.userId);
            int liveVillageId = user.villageId;
            if (activeConnection != null) {
                liveVillageId = ACTIVE_VILLAGE_BY_CONNECTION.getOrDefault(activeConnection.getID(), user.villageId);
            }
            list.add(new com.slx.nord.jgnpersistentobjectdetails.IMListPlayerData(
                user.userId,
                user.username,
                activeConnection != null,
                DATABASE.getPlayerAgeYears(user.userId),
                DATABASE.getPlayerSex(user.userId),
                liveVillageId,
                villageVisitorCount(liveVillageId),
                "",
                "",
                "",
                resolvePlayerTheme(user),
                user.level
            ));
        }
        connection.sendTCP(new RequestIMListResponseMessage(playerId, list));
    }

    private static void notifyGuestbookEntryAdded(int ownerPlayerId, int fromPlayerId, String fromPlayerName) {
        if (ownerPlayerId <= 0 || fromPlayerId <= 0 || ownerPlayerId == fromPlayerId) {
            return;
        }
        Connection ownerConnection = findConnectionByPlayerId(ownerPlayerId);
        if (ownerConnection == null) {
            return;
        }
        ownerConnection.sendTCP(new AreNewGuestbookEntriesAvailableResponseMessage(true));
        try {
            ownerConnection.sendTCP(
                com.slx.nord.jgnmessages.clientinbound.GenericByteArrayMessage.createSerializedByteArrayMessage(
                    new com.slx.nord.jgnmessages.bytearraymessages.ReceivedNewGuestbookEntryMessage(fromPlayerId, safe(fromPlayerName))
                )
            );
        } catch (Exception e) {
            log("[Server] Failed to send realtime guestbook notification: " + e.getMessage());
        }
    }

    private static RequestGuestbookEntriesResponseMessage buildGuestbookEntriesResponse(int ownerPlayerId,
                                                                                         String ownerName,
                                                                                         NordDatabase.GuestbookPage page) {
        GuestbookResponseLists lists = buildGuestbookLists(page);
        return new RequestGuestbookEntriesResponseMessage(
            page.hasNewerPosts,
            page.hasOlderPosts,
            safe(ownerName),
            "",
            0,
            resolvePlayerTheme(ownerPlayerId),
            ownerPlayerId,
            lists.postIds,
            lists.posterNames,
            lists.postTexts,
            lists.posterAges,
            lists.posterSexes,
            lists.posterUserIds,
            lists.posterPhotoURLs,
            lists.postTimes,
            lists.posterIsOnline,
            lists.privates,
            lists.levels,
            lists.types
        );
    }

    private static RequestGuestbookHistoryResponseMessage buildGuestbookHistoryResponse(int offset,
                                                                                         int player1,
                                                                                         int player2,
                                                                                         NordDatabase.GuestbookPage page) {
        GuestbookResponseLists lists = buildGuestbookLists(page);
        return new RequestGuestbookHistoryResponseMessage(
            page.hasNewerPosts,
            page.hasOlderPosts,
            lists.postIds,
            lists.posterNames,
            lists.postTexts,
            lists.posterAges,
            lists.posterSexes,
            lists.posterUserIds,
            lists.posterPhotoURLs,
            lists.postTimes,
            lists.posterIsOnline,
            lists.privates,
            offset,
            player1,
            player2,
            lists.levels,
            lists.types
        );
    }

    private static GuestbookResponseLists buildGuestbookLists(NordDatabase.GuestbookPage page) {
        GuestbookResponseLists lists = new GuestbookResponseLists();
        if (page == null || page.entries == null) {
            return lists;
        }
        for (NordDatabase.GuestbookEntryRecord entry : page.entries) {
            if (entry == null) {
                continue;
            }
            lists.postIds.add(entry.postId);
            lists.posterNames.add(safe(entry.posterName));
            lists.postTexts.add(safe(entry.postText));
            lists.posterAges.add(0);
            lists.posterSexes.add(DATABASE.getPlayerSex(entry.posterPlayerId));
            lists.posterUserIds.add(entry.posterPlayerId);
            lists.posterPhotoURLs.add("");
            lists.postTimes.add(entry.postTime);
            lists.posterIsOnline.add(findConnectionByPlayerId(entry.posterPlayerId) != null);
            lists.privates.add(entry.isPrivate);
            lists.levels.add(entry.posterLevel);
            lists.types.add(safe(entry.type));
        }
        return lists;
    }

    private static com.nordclientpc.eM buildBadgeDataForPlayer(int playerId) {
        ConcurrentHashMap<Integer, com.nordclientpc.Ul> badgeMap = new ConcurrentHashMap<>();
        if (playerId <= 0) {
            return new com.nordclientpc.eM(badgeMap, false);
        }

        boolean hasVip = false;
        ArrayList<NordDatabase.AchievementRecord> achievements = DATABASE.loadAchievements(playerId);
        for (NordDatabase.AchievementRecord achievement : achievements) {
            int key = (achievement.category * 10000) + achievement.type;
            badgeMap.put(key, new com.nordclientpc.Ul(achievement.value));
            if (achievement.category == ACHIEVEMENT_ROLE_CATEGORY &&
                achievement.type == ACHIEVEMENT_VIP_TYPE &&
                achievement.value > 0) {
                hasVip = true;
            }
        }
        return new com.nordclientpc.eM(badgeMap, hasVip);
    }

    private static Path resolveRandomseedPath() {
        String fromProperty = System.getProperty("nord.randomseed.path");
        if (!safe(fromProperty).isEmpty()) {
            return Paths.get(fromProperty);
        }
        String fromEnv = System.getenv("NORD_RANDOMSEED_PATH");
        if (!safe(fromEnv).isEmpty()) {
            return Paths.get(fromEnv);
        }
        Path cwdSeed = Paths.get(DEFAULT_RANDOMSEED_PATH);
        if (Files.isRegularFile(cwdSeed)) {
            return cwdSeed;
        }
        Path parentSeed = Paths.get("..", DEFAULT_RANDOMSEED_PATH);
        if (Files.isRegularFile(parentSeed)) {
            return parentSeed;
        }
        return cwdSeed;
    }

    private static BuildingEconomyCatalog loadBuildingEconomyCatalog(Path preferredPath) {
        for (Path candidate : buildPriceSourceCandidates(preferredPath)) {
            BuildingEconomyCatalog loaded = tryLoadBuildingEconomyCatalogFromPath(candidate);
            if (!loaded.pricesByType.isEmpty() ||
                !loaded.treasureTypesByType.isEmpty() ||
                !loaded.giftBuildingTypes.isEmpty() ||
                !loaded.resourceHarvestBuildingTypes.isEmpty()) {
                log("[Server] Loaded building economy data from " + candidate.toAbsolutePath());
                return loaded;
            }
        }
        return new BuildingEconomyCatalog(new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashSet<>(), new HashSet<>());
    }

    private static Set<Path> buildPriceSourceCandidates(Path preferredPath) {
        LinkedHashSet<Path> candidates = new LinkedHashSet<>();
        addPriceSourceCandidate(candidates, preferredPath);
        addPriceSourceCandidate(candidates, Paths.get(DEFAULT_RANDOMSEED_PATH));
        addPriceSourceCandidate(candidates, Paths.get("..", DEFAULT_RANDOMSEED_PATH));
        addPriceSourceCandidate(candidates, Paths.get("decrypt", "resources.xml"));
        addPriceSourceCandidate(candidates, Paths.get("..", "decrypt", "resources.xml"));
        return candidates;
    }

    private static void addPriceSourceCandidate(Set<Path> candidates, Path candidate) {
        if (candidate == null) {
            return;
        }
        candidates.add(candidate.toAbsolutePath().normalize());
    }

    private static BuildingEconomyCatalog tryLoadBuildingEconomyCatalogFromPath(Path candidate) {
        BuildingEconomyCatalog empty =
            new BuildingEconomyCatalog(
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptySet(),
                Collections.emptySet()
            );
        if (candidate == null || !Files.isRegularFile(candidate)) {
            return empty;
        }
        try {
            byte[] rawBytes = Files.readAllBytes(candidate);

            BuildingEconomyCatalog plainParsed = parseBuildingEconomyCatalogFromText(new String(rawBytes, StandardCharsets.UTF_8));
            if (!plainParsed.pricesByType.isEmpty() ||
                !plainParsed.treasureTypesByType.isEmpty() ||
                !plainParsed.resourceHarvestBuildingTypes.isEmpty()) {
                return plainParsed;
            }

            byte[] decryptedBytes;
            try {
                decryptedBytes = decryptRandomseedBytes(rawBytes);
            } catch (Exception ignored) {
                return empty;
            }
            return parseBuildingEconomyCatalogFromText(new String(decryptedBytes, StandardCharsets.UTF_8));
        } catch (IOException e) {
            log("[Server] Failed reading building price source " + candidate.toAbsolutePath() + ": " + e.getMessage());
            return empty;
        }
    }

    private static BuildingEconomyCatalog parseBuildingEconomyCatalogFromText(String text) {
        Map<Short, Integer> prices = new HashMap<>();
        Map<Short, TreasureType> treasureTypes = new HashMap<>();
        Map<Short, Integer> requiredIngredientCounts = new HashMap<>();
        Set<Short> giftBuildingTypes = new HashSet<>();
        Set<Short> resourceHarvestBuildingTypes = new HashSet<>();
        if (safe(text).isEmpty()) {
            return new BuildingEconomyCatalog(
                prices,
                treasureTypes,
                requiredIngredientCounts,
                giftBuildingTypes,
                resourceHarvestBuildingTypes
            );
        }
        Map<Integer, Integer> ingredientCountByRecipeId = new HashMap<>();
        Matcher recipeTagMatcher = RECIPE_TAG_PATTERN.matcher(text);
        while (recipeTagMatcher.find()) {
            String recipeTag = recipeTagMatcher.group();
            Integer recipeId = null;
            int ingredientCount = 0;
            Matcher attrMatcher = ATTRIBUTE_PATTERN.matcher(recipeTag);
            while (attrMatcher.find()) {
                String key = safe(attrMatcher.group(1)).toLowerCase(Locale.ROOT);
                String value = attrMatcher.group(2);
                if ("id".equals(key)) {
                    recipeId = parseNonNegativeIntValue(value);
                } else if ("ingredient".equals(key)) {
                    ingredientCount++;
                }
            }
            if (recipeId != null && recipeId >= 0) {
                ingredientCountByRecipeId.put(recipeId, Math.max(0, ingredientCount));
            }
        }

        Matcher tagMatcher = BUILDING_TAG_PATTERN.matcher(text);
        while (tagMatcher.find()) {
            String tag = tagMatcher.group();
            Short buildingType = null;
            Integer basePrice = null;
            TreasureType treasureType = null;
            Integer recipeId = null;
            boolean isGiftBuildingType = false;
            boolean isResourceClickBuildingType = false;
            boolean consumeRemovesBuilding = false;
            Matcher attrMatcher = ATTRIBUTE_PATTERN.matcher(tag);
            while (attrMatcher.find()) {
                String key = safe(attrMatcher.group(1)).toLowerCase(Locale.ROOT);
                String value = attrMatcher.group(2);
                if ("id".equals(key)) {
                    buildingType = parseShortValue(value);
                } else if ("price".equals(key)) {
                    basePrice = parseNonNegativeIntValue(value);
                } else if ("treasure".equals(key)) {
                    treasureType = parseTreasureType(value);
                } else if ("recipe".equals(key)) {
                    recipeId = parseNonNegativeIntValue(value);
                } else if ("gift".equals(key)) {
                    if (isTrueFlagValue(value)) {
                        isGiftBuildingType = true;
                    }
                } else if ("click".equals(key)) {
                    if ("gift".equalsIgnoreCase(safe(value).trim())) {
                        isGiftBuildingType = true;
                    }
                    if ("resource".equalsIgnoreCase(safe(value).trim())) {
                        isResourceClickBuildingType = true;
                    }
                } else if ("consume".equals(key)) {
                    if ("remove".equalsIgnoreCase(safe(value).trim())) {
                        consumeRemovesBuilding = true;
                    }
                }
            }
            if (buildingType != null && basePrice != null) {
                prices.put(buildingType, basePrice);
            }
            if (buildingType != null && treasureType != null) {
                treasureTypes.put(buildingType, treasureType);
            }
            if (buildingType != null && recipeId != null && recipeId >= 0) {
                int requiredIngredients = Math.max(0, ingredientCountByRecipeId.getOrDefault(recipeId, 0));
                requiredIngredientCounts.put(buildingType, requiredIngredients);
            }
            if (buildingType != null && isGiftBuildingType) {
                giftBuildingTypes.add(buildingType);
            }
            if (buildingType != null && isResourceClickBuildingType && consumeRemovesBuilding) {
                resourceHarvestBuildingTypes.add(buildingType);
            }
        }
        return new BuildingEconomyCatalog(
            prices,
            treasureTypes,
            requiredIngredientCounts,
            giftBuildingTypes,
            resourceHarvestBuildingTypes
        );
    }

    private static CompanionCatalog loadCompanionCatalog(Path preferredPath) {
        for (Path candidate : buildPriceSourceCandidates(preferredPath)) {
            CompanionCatalog loaded = tryLoadCompanionCatalogFromPath(candidate);
            if (!loaded.mountsByType.isEmpty() || !loaded.petsByType.isEmpty()) {
                log("[Server] Loaded companion economy data from " + candidate.toAbsolutePath());
                return loaded;
            }
        }
        return new CompanionCatalog(Collections.emptyMap(), Collections.emptyMap());
    }

    private static CompanionCatalog tryLoadCompanionCatalogFromPath(Path candidate) {
        CompanionCatalog empty = new CompanionCatalog(Collections.emptyMap(), Collections.emptyMap());
        if (candidate == null || !Files.isRegularFile(candidate)) {
            return empty;
        }
        try {
            byte[] rawBytes = Files.readAllBytes(candidate);

            CompanionCatalog plainParsed = parseCompanionCatalogFromText(new String(rawBytes, StandardCharsets.UTF_8));
            if (!plainParsed.mountsByType.isEmpty() || !plainParsed.petsByType.isEmpty()) {
                return plainParsed;
            }

            byte[] decryptedBytes;
            try {
                decryptedBytes = decryptRandomseedBytes(rawBytes);
            } catch (Exception ignored) {
                return empty;
            }
            return parseCompanionCatalogFromText(new String(decryptedBytes, StandardCharsets.UTF_8));
        } catch (IOException e) {
            log("[Server] Failed reading companion source " + candidate.toAbsolutePath() + ": " + e.getMessage());
            return empty;
        }
    }

    private static CompanionCatalog parseCompanionCatalogFromText(String text) {
        Map<Short, CompanionCatalogEntry> mounts = parseCompanionCatalogEntries(text, HORSE_TAG_PATTERN, "mount");
        Map<Short, CompanionCatalogEntry> pets = parseCompanionCatalogEntries(text, PET_TAG_PATTERN, "pet");
        return new CompanionCatalog(mounts, pets);
    }

    private static Map<Short, CompanionCatalogEntry> parseCompanionCatalogEntries(String text,
                                                                                  Pattern tagPattern,
                                                                                  String kind) {
        Map<Short, CompanionCatalogEntry> entries = new HashMap<>();
        if (safe(text).isEmpty()) {
            return entries;
        }
        Matcher tagMatcher = tagPattern.matcher(text);
        while (tagMatcher.find()) {
            String tag = tagMatcher.group();
            Short companionType = null;
            Integer price = null;
            Integer level = null;
            int canBuyFrom = 0;
            int canBuyTo = 0;
            Matcher attrMatcher = ATTRIBUTE_PATTERN.matcher(tag);
            while (attrMatcher.find()) {
                String key = safe(attrMatcher.group(1)).toLowerCase(Locale.ROOT);
                String value = attrMatcher.group(2);
                if ("id".equals(key)) {
                    companionType = parseShortValue(value);
                } else if ("price".equals(key)) {
                    price = parseNonNegativeIntValue(value);
                } else if ("level".equals(key)) {
                    level = parseNonNegativeIntValue(value);
                } else if ("canbuyfrom".equals(key)) {
                    canBuyFrom = parseMonthDayToken(value);
                } else if ("canbuyto".equals(key)) {
                    canBuyTo = parseMonthDayToken(value);
                }
            }
            if (companionType == null || companionType <= 0) {
                continue;
            }
            CompanionCatalogEntry entry = new CompanionCatalogEntry(
                kind,
                companionType,
                price == null ? 0 : price,
                level == null ? 0 : level,
                canBuyFrom,
                canBuyTo
            );
            entries.put(companionType, entry);
        }
        return entries;
    }

    private static CompanionCatalogEntry resolveCompanionCatalogEntry(String kind, short companionType) {
        if (companionType <= 0) {
            return null;
        }
        String normalizedKind = normalizeCompanionKind(kind);
        if ("pet".equals(normalizedKind)) {
            return PET_CATALOG_BY_TYPE.get(companionType);
        }
        if ("mount".equals(normalizedKind)) {
            return MOUNT_CATALOG_BY_TYPE.get(companionType);
        }
        return null;
    }

    private static CompanionPurchaseRequest parseCompanionPurchaseRequest(String logMessage) {
        Matcher matcher = COMPANION_PURCHASE_LOG_PATTERN.matcher(safe(logMessage).trim());
        if (!matcher.find()) {
            return null;
        }
        String kind = normalizeCompanionKind(matcher.group(1));
        Short companionType = parseShortValue(matcher.group(2));
        if (kind.isEmpty() || companionType == null || companionType <= 0) {
            return null;
        }
        return new CompanionPurchaseRequest(kind, companionType);
    }

    private static boolean canStoreCompanion(NordDatabase.UserRecord user, String kind, short companionType) {
        if (user == null || companionType <= 0) {
            return true;
        }
        CompanionCatalogEntry entry = resolveCompanionCatalogEntry(kind, companionType);
        if (entry == null) {
            return false;
        }
        if (entry.price <= 0) {
            return true;
        }
        if (DATABASE.isCompanionOwned(user.userId, kind, companionType)) {
            return true;
        }
        AvatarDetails currentAvatar = DATABASE.loadAvatarDetails(user.userId, user.username, user.level);
        short equippedType = "pet".equals(normalizeCompanionKind(kind))
            ? currentAvatar.getPetType()
            : currentAvatar.getMountType();
        if (equippedType == companionType) {
            try {
                DATABASE.grantCompanionOwnership(user.userId, kind, companionType, "avatar_state");
            } catch (IOException ignored) {
                // Keep the equip allowed even if ownership backfill logging fails.
            }
            return true;
        }
        return false;
    }

    private static String normalizeCompanionKind(String kind) {
        String normalized = safe(kind).trim().toLowerCase(Locale.ROOT);
        if ("horse".equals(normalized) || "mount".equals(normalized)) {
            return "mount";
        }
        if ("pet".equals(normalized)) {
            return "pet";
        }
        return "";
    }

    private static int parseMonthDayToken(String value) {
        String normalized = safe(value).trim();
        if (!normalized.matches("\\d{4}")) {
            return 0;
        }
        Integer parsed = parseNonNegativeIntValue(normalized);
        if (parsed == null || parsed <= 0) {
            return 0;
        }
        int month = parsed / 100;
        int day = parsed % 100;
        if (month < 1 || month > 12 || day < 1 || day > 31) {
            return 0;
        }
        return parsed;
    }

    private static boolean isCompanionSeasonAvailable(CompanionCatalogEntry entry, LocalDate today) {
        if (entry == null) {
            return false;
        }
        int from = entry.canBuyFromMonthDay;
        int to = entry.canBuyToMonthDay;
        if (from <= 0 || to <= 0) {
            return true;
        }
        LocalDate resolvedToday = today == null ? LocalDate.now() : today;
        int current = Math.max(0, resolvedToday.getMonthValue() * 100 + resolvedToday.getDayOfMonth());
        if (from <= to) {
            return current >= from && current <= to;
        }
        return current >= from || current <= to;
    }

    private static byte[] decryptRandomseedBytes(byte[] encryptedBytes) throws Exception {
        PBEKeySpec keySpec = new PBEKeySpec(RANDOMSEED_PASSWORD.toCharArray());
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
        SecretKey secretKey = keyFactory.generateSecret(keySpec);
        PBEParameterSpec parameterSpec = new PBEParameterSpec(RANDOMSEED_SALT, RANDOMSEED_ITERATIONS);
        Cipher cipher = Cipher.getInstance("PBEWithMD5AndDES");
        cipher.init(Cipher.DECRYPT_MODE, secretKey, parameterSpec);
        return cipher.doFinal(encryptedBytes);
    }

    private static Short parseShortValue(String text) {
        if (safe(text).isEmpty()) {
            return null;
        }
        try {
            int value = Integer.parseInt(text.trim());
            if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
                return null;
            }
            return (short) value;
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static TreasureType parseTreasureType(String text) {
        if (safe(text).isEmpty()) {
            return null;
        }
        try {
            return TreasureType.valueOf(text.trim().toUpperCase());
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    private static boolean isTrueFlagValue(String text) {
        String normalized = safe(text).trim().toLowerCase(Locale.ROOT);
        return "1".equals(normalized) || "true".equals(normalized) || "yes".equals(normalized);
    }

    private static TreasurePickupReward rollTreasureReward(TreasureType configuredTreasureType, short buildingType) {
        if (configuredTreasureType == null) {
            return new TreasurePickupReward(null, 0);
        }
        switch (configuredTreasureType) {
            case COINS:
                return new TreasurePickupReward(TreasureType.COINS, rollCoinTreasureAmount(buildingType));
            case WEATHER:
                double weatherRoll = ThreadLocalRandom.current().nextDouble();
                if (weatherRoll <= 0.33d) {
                    return new TreasurePickupReward(TreasureType.SUNSHINE, 1);
                }
                if (weatherRoll <= 0.66d) {
                    return new TreasurePickupReward(TreasureType.RAIN, 1);
                }
                return new TreasurePickupReward(TreasureType.LIGHTNING, 1);
            case SUNSHINE:
            case RAIN:
            case LIGHTNING:
                return new TreasurePickupReward(configuredTreasureType, 1);
            default:
                return new TreasurePickupReward(configuredTreasureType, 0);
        }
    }

    private static int rollCoinTreasureAmount(short buildingType) {
        double roll = ThreadLocalRandom.current().nextDouble();
        int type = buildingType;
        if (type == 3407 || type == 3408) {
            return 1000;
        }
        if (type == 477 || type == 3021) {
            if (roll > 0.97d) {
                return 2500;
            }
            if (roll > 0.95d) {
                return 1250;
            }
            if (roll > 0.85d) {
                return 600;
            }
            if (roll > 0.8d) {
                return 300;
            }
            if (roll > 0.65d) {
                return 250;
            }
            if (roll > 0.5d) {
                return 200;
            }
            if (roll > 0.3d) {
                return 150;
            }
            return 100;
        }

        if (roll > 0.999d) {
            return 500;
        }
        if (roll > 0.98d) {
            return 250;
        }
        if (roll > 0.9d) {
            return 100;
        }
        if (roll > 0.7d) {
            return 50;
        }
        if (roll > 0.4d) {
            return 20;
        }
        if (roll > 0.1d) {
            return 15;
        }
        return 0;
    }

    private static void sendPickedUpTreasureMessage(Connection connection,
                                                    TreasureType treasureType,
                                                    int totalCreditsForPlayer,
                                                    int coinsInTreasure) {
        if (connection == null || treasureType == null) {
            return;
        }
        try {
            connection.sendTCP(
                com.slx.nord.jgnmessages.clientinbound.GenericByteArrayMessage.createSerializedByteArrayMessage(
                    new PickedUpTreasureMessage(treasureType, totalCreditsForPlayer, coinsInTreasure)
                )
            );
        } catch (Exception e) {
            log("[Server] Failed to send PickedUpTreasureMessage: " + e.getMessage());
        }
    }

    private static void sendBuildingAddRejected(Connection connection, int villageId, short buildingId, String reason) {
        if (connection == null || villageId <= 0) {
            return;
        }
        sendGenericByteArraySerializable(
            connection,
            new BuildingNotAddedNotEnoughCreditsMessage(villageId, buildingId),
            "BuildingNotAddedNotEnoughCreditsMessage(" + safe(reason) + ")"
        );
    }

    private static long calculateBuildingsChecksum(int villageId) {
        long checksum = Long.MIN_VALUE;
        ArrayList<Building> buildings = DATABASE.loadBuildings(villageId);
        for (Building building : buildings) {
            if (building == null) {
                continue;
            }
            checksum += (long) building.getBuildingID();
            checksum += building.isConsumed() ? 1L : 0L;
            checksum += (long) ((byte) building.getTileX());
            checksum += (long) ((byte) building.getTileZ());
        }
        return checksum;
    }

    private static Integer parseNonNegativeIntValue(String text) {
        if (safe(text).isEmpty()) {
            return null;
        }
        try {
            int value = Integer.parseInt(text.trim());
            return Math.max(0, value);
        } catch (NumberFormatException ignored) {
            try {
                double value = Double.parseDouble(text.trim());
                if (Double.isNaN(value) || Double.isInfinite(value)) {
                    return null;
                }
                return Math.max(0, (int) Math.round(value));
            } catch (NumberFormatException ignoredAgain) {
                return null;
            }
        }
    }

    private static int resolveBuyCreditsAmount(String code, String logMessage) {
        String normalizedCode = normalizeBuyCreditsCode(code);
        if (!normalizedCode.isEmpty()) {
            Integer configured = BUY_CREDITS_AMOUNT_BY_CODE.get(normalizedCode);
            if (configured != null && configured > 0) {
                return configured;
            }
            Integer parsedCode = parseNonNegativeIntValue(normalizedCode);
            if (parsedCode != null && parsedCode > 0) {
                return parsedCode;
            }
            Integer kAmountFromCode = parseCreditsWithKSuffix(code);
            if (kAmountFromCode != null && kAmountFromCode > 0) {
                return kAmountFromCode;
            }
        }
        Integer kAmountFromLog = parseCreditsWithKSuffix(logMessage);
        if (kAmountFromLog != null && kAmountFromLog > 0) {
            return kAmountFromLog;
        }
        String normalizedLog = safe(logMessage).trim().toLowerCase(Locale.ROOT);
        if (normalizedLog.contains("sms") && !normalizedCode.isEmpty()) {
            return DEFAULT_SMS_BUY_CREDITS_AMOUNT;
        }
        return -1;
    }

    private static long resolveWildResourceRespawnDelayMs() {
        String raw = safe(System.getProperty("nord.wild.resource.respawn.ms")).trim();
        if (raw.isEmpty()) {
            raw = safe(System.getenv("NORD_WILD_RESOURCE_RESPAWN_MS")).trim();
        }
        Integer parsed = parseNonNegativeIntValue(raw);
        if (parsed == null || parsed <= 0) {
            return DEFAULT_WILD_RESOURCE_RESPAWN_DELAY_MS;
        }
        return parsed.longValue();
    }

    private static Map<String, Integer> resolveBuyCreditsCodeMap() {
        HashMap<String, Integer> parsed = new HashMap<>();
        parsed.put("SMS1K", 1000);
        parsed.put("PAYNOVA4K", 4000);
        parsed.put("PAYNOVA9K", 9000);
        parsed.put("PAYNOVA15K", 15000);
        parsed.put("PAYPAL1K", 1000);
        parsed.put("PAYPAL4K", 4000);
        parsed.put("PAYPAL9K", 9000);
        parsed.put("PAYPAL15K", 15000);

        String raw = safe(System.getProperty("nord.buycredits.code.map")).trim();
        if (raw.isEmpty()) {
            raw = safe(System.getenv("NORD_BUY_CREDITS_CODE_MAP")).trim();
        }
        if (!raw.isEmpty()) {
            String[] entries = raw.split("[,;]");
            for (String entry : entries) {
                if (entry == null) {
                    continue;
                }
                String[] parts = entry.split("=", 2);
                if (parts.length != 2) {
                    continue;
                }
                String key = normalizeBuyCreditsCode(parts[0]);
                Integer value = parseNonNegativeIntValue(parts[1]);
                if (key.isEmpty() || value == null || value <= 0) {
                    continue;
                }
                parsed.put(key, value);
            }
        }
        return Collections.unmodifiableMap(parsed);
    }

    private static String resolvePlayerImagesPrefix() {
        String configuredPrefix = safe(System.getProperty("nord.image.prefix")).trim();
        if (configuredPrefix.isEmpty()) {
            configuredPrefix = safe(System.getenv("NORD_IMAGE_PREFIX")).trim();
        }
        if (!configuredPrefix.isEmpty()) {
            return ensureTrailingSlash(configuredPrefix);
        }

        String host = safe(System.getProperty("nord.server.host")).trim();
        if (host.isEmpty()) {
            host = safe(System.getenv("NORD_SERVER_HOST")).trim();
        }
        if (host.isEmpty()) {
            host = "127.0.0.1";
        }

        String imagePort = safe(System.getProperty("nord.server.image")).trim();
        if (imagePort.isEmpty()) {
            imagePort = safe(System.getProperty("nord.image.port")).trim();
        }
        if (imagePort.isEmpty()) {
            imagePort = safe(System.getenv("NORD_SERVER_IMAGE_PORT")).trim();
        }
        if (imagePort.isEmpty()) {
            imagePort = safe(System.getenv("NORD_IMAGE_PORT")).trim();
        }
        if (imagePort.isEmpty()) {
            imagePort = "31876";
        }
        return "http://" + host + ":" + imagePort + "/dl/";
    }

    private static String ensureTrailingSlash(String value) {
        return value.endsWith("/") ? value : value + "/";
    }

    private static String normalizeBuyCreditsCode(String code) {
        if (safe(code).isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        String trimmed = code.trim();
        for (int i = 0; i < trimmed.length(); i++) {
            char ch = trimmed.charAt(i);
            if (Character.isLetterOrDigit(ch)) {
                builder.append(Character.toUpperCase(ch));
            }
        }
        return builder.toString();
    }

    private static Integer parseCreditsWithKSuffix(String text) {
        String value = safe(text);
        if (value.isEmpty()) {
            return null;
        }
        Matcher matcher = CREDITS_WITH_K_PATTERN.matcher(value);
        if (!matcher.find()) {
            return null;
        }
        Integer base = parseNonNegativeIntValue(matcher.group(1));
        if (base == null || base <= 0) {
            return null;
        }
        return base * 1000;
    }

    private static BuyCreditsSource resolveBuyCreditsSource(String code, String logMessage) {
        String combined = (safe(code) + " " + safe(logMessage)).toLowerCase(Locale.ROOT);
        if (combined.contains("sponsorpay")) {
            return BuyCreditsSource.SPONSORPAY;
        }
        if (combined.contains("superrewards") || combined.contains("super_rewards")) {
            return BuyCreditsSource.SUPERREWARDS;
        }
        if (combined.contains("facebook") || combined.contains("fb_like") || combined.contains("liked")) {
            return BuyCreditsSource.FACEBOOK;
        }
        if (combined.contains("teambonus") || combined.contains("team_bonus")) {
            return BuyCreditsSource.TEAM_BONUS;
        }
        return BuyCreditsSource.UNKNOWN;
    }

    private static void sendBuyCreditsNotification(Connection connection,
                                                   NordDatabase.UserRecord user,
                                                   int boughtCredits,
                                                   int totalCredits,
                                                   String code,
                                                   String logMessage) {
        if (connection == null || boughtCredits <= 0) {
            return;
        }
        BuyCreditsSource source = resolveBuyCreditsSource(code, logMessage);
        if (source == BuyCreditsSource.SPONSORPAY) {
            sendGenericByteArraySerializable(
                connection,
                new SponsorpayNotificationMessage(boughtCredits, totalCredits),
                "SponsorpayNotificationMessage"
            );
            return;
        }
        if (source == BuyCreditsSource.SUPERREWARDS) {
            sendGenericByteArraySerializable(
                connection,
                new SuperRewardsNotificationMessage(boughtCredits, totalCredits),
                "SuperRewardsNotificationMessage"
            );
            return;
        }
        if (source == BuyCreditsSource.FACEBOOK) {
            sendGenericByteArraySerializable(
                connection,
                new LikedOnFacebookEventMessage(boughtCredits),
                "LikedOnFacebookEventMessage"
            );
            return;
        }
        if (source == BuyCreditsSource.TEAM_BONUS) {
            sendBuyCreditsTeamBonus(connection, user, boughtCredits, logMessage);
        }
    }

    private static void sendBuyCreditsTeamBonus(Connection connection,
                                                NordDatabase.UserRecord user,
                                                int boughtCredits,
                                                String logMessage) {
        int bonusId = DEFAULT_TEAM_BONUS_ID;
        String fromPlayer = user != null ? safe(user.username) : "";
        int otherBonusReceivers = 0;
        int levelAchieved = user != null ? Math.max(1, user.level) : 1;
        int data = boughtCredits;

        String[] parts = safe(logMessage).split(":");
        if (parts.length >= 6 && "teambonus".equalsIgnoreCase(parts[0].trim())) {
            Integer parsedBonusId = parseNonNegativeIntValue(parts[1]);
            Integer parsedOtherBonusReceivers = parseNonNegativeIntValue(parts[3]);
            Integer parsedLevelAchieved = parseNonNegativeIntValue(parts[4]);
            Integer parsedData = parseNonNegativeIntValue(parts[5]);
            if (parsedBonusId != null && parsedBonusId > 0) {
                bonusId = parsedBonusId;
            }
            String parsedFromPlayer = safe(parts[2]).trim();
            if (!parsedFromPlayer.isEmpty()) {
                fromPlayer = parsedFromPlayer;
            }
            if (parsedOtherBonusReceivers != null) {
                otherBonusReceivers = parsedOtherBonusReceivers;
            }
            if (parsedLevelAchieved != null && parsedLevelAchieved > 0) {
                levelAchieved = parsedLevelAchieved;
            }
            if (parsedData != null && parsedData > 0) {
                data = parsedData;
            }
        }
        sendGenericByteArraySerializable(
            connection,
            new TeamBonusMessage(bonusId, fromPlayer, otherBonusReceivers, levelAchieved, data),
            "TeamBonusMessage"
        );
    }

    private static String buildShareAction(int playerId, short shareType, long shareData, long shareId) {
        if (shareType != SHARE_TYPE_RESOURCE) {
            return "";
        }
        long resolvedShareId = shareId > 0L ? shareId : Math.abs(ThreadLocalRandom.current().nextLong());
        long entropy = ThreadLocalRandom.current().nextLong();
        return Long.toHexString(playerId & 0xffffffffL) +
               Long.toHexString(resolvedShareId) +
               Long.toHexString(shareData) +
               Long.toHexString(entropy);
    }

    private static String safe(String s) {
        return s == null ? "" : s;
    }

    private static String normalizeStatusText(String text) {
        String normalized = safe(text);
        if (normalized.length() > 100) {
            return normalized.substring(0, 100);
        }
        return normalized;
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

    private static int clampToInt(long value) {
        if (value > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        if (value < Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        }
        return (int) value;
    }

    private static long safeAddMillis(long baseMillis, long offsetMillis) {
        long normalizedOffset = Math.max(0L, offsetMillis);
        if (Long.MAX_VALUE - baseMillis < normalizedOffset) {
            return Long.MAX_VALUE;
        }
        return baseMillis + normalizedOffset;
    }

    private static int[] resolveLevelUpRewardConfig() {
        int rewardBase = DEFAULT_LEVEL_UP_REWARD_BASE_CREDITS;
        int rewardStep = DEFAULT_LEVEL_UP_REWARD_STEP_CREDITS;

        // Backwards compatibility: a legacy flat reward override maps to base=<value>, step=0.
        String legacyProperty = System.getProperty("nord.levelup.reward");
        Integer parsedLegacyProperty = parseNonNegativeIntValue(legacyProperty);
        if (parsedLegacyProperty != null) {
            rewardBase = parsedLegacyProperty;
            rewardStep = 0;
        } else {
            String legacyEnv = System.getenv("NORD_LEVELUP_REWARD");
            Integer parsedLegacyEnv = parseNonNegativeIntValue(legacyEnv);
            if (parsedLegacyEnv != null) {
                rewardBase = parsedLegacyEnv;
                rewardStep = 0;
            }
        }

        Integer parsedBaseProperty = parseNonNegativeIntValue(System.getProperty("nord.levelup.reward.base"));
        if (parsedBaseProperty != null) {
            rewardBase = parsedBaseProperty;
        } else {
            Integer parsedBaseEnv = parseNonNegativeIntValue(System.getenv("NORD_LEVELUP_REWARD_BASE"));
            if (parsedBaseEnv != null) {
                rewardBase = parsedBaseEnv;
            }
        }

        Integer parsedStepProperty = parseNonNegativeIntValue(System.getProperty("nord.levelup.reward.step"));
        if (parsedStepProperty != null) {
            rewardStep = parsedStepProperty;
        } else {
            Integer parsedStepEnv = parseNonNegativeIntValue(System.getenv("NORD_LEVELUP_REWARD_STEP"));
            if (parsedStepEnv != null) {
                rewardStep = parsedStepEnv;
            }
        }

        return new int[] {rewardBase, rewardStep};
    }

    private static boolean resolveBuildingsChecksumResponsesEnabled() {
        String property = safe(System.getProperty("nord.buildings.checksum.enabled")).trim();
        if (!property.isEmpty()) {
            return isTruthy(property);
        }
        String env = safe(System.getenv("NORD_BUILDINGS_CHECKSUM_ENABLED")).trim();
        if (!env.isEmpty()) {
            return isTruthy(env);
        }
        return false;
    }

    private static boolean resolveRequireProviderCallbackBuyCredits() {
        String property = safe(System.getProperty("nord.buycredits.require.providercallback")).trim();
        if (!property.isEmpty()) {
            return isTruthy(property);
        }
        String env = safe(System.getenv("NORD_BUY_CREDITS_REQUIRE_PROVIDER_CALLBACK")).trim();
        if (!env.isEmpty()) {
            return isTruthy(env);
        }
        return false;
    }

    private static boolean isTruthy(String value) {
        String normalized = safe(value).trim().toLowerCase(Locale.ROOT);
        return "1".equals(normalized) ||
               "true".equals(normalized) ||
               "yes".equals(normalized) ||
               "on".equals(normalized);
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
