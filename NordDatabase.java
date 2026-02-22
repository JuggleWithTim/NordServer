import com.slx.nord.jgnpersistentobjectdetails.AvatarDetails;
import com.slx.nord.jgnpersistentobjectdetails.Building;
import com.slx.nord.jgnpersistentobjectdetails.Tree;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Driver;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

public class NordDatabase {
    private static final int HEIGHTMAP_PRESET_COUNT = 50;
    private static final int PASSWORD_PBKDF2_ITERATIONS = 120000;
    private static final int PASSWORD_SALT_BYTES = 16;
    private static final int PASSWORD_HASH_BYTES = 32;
    private static final SecureRandom PASSWORD_RANDOM = new SecureRandom();

    public static final class UserRecord {
        public final int userId;
        public final String username;
        public final String passwordHash;
        public final int villageId;
        public final String villageName;
        public final int level;
        public final int heightmapPresetId;
        public final int slxCredits;

        private UserRecord(int userId, String username, String passwordHash, int villageId, String villageName, int level, int heightmapPresetId, int slxCredits) {
            this.userId = userId;
            this.username = username;
            this.passwordHash = passwordHash;
            this.villageId = villageId;
            this.villageName = villageName;
            this.level = level;
            this.heightmapPresetId = heightmapPresetId;
            this.slxCredits = slxCredits;
        }
    }

    public static final class CreateUserResult {
        public final boolean success;
        public final boolean usernameTaken;
        public final UserRecord user;

        public CreateUserResult(boolean success, boolean usernameTaken, UserRecord user) {
            this.success = success;
            this.usernameTaken = usernameTaken;
            this.user = user;
        }
    }

    public static final class AchievementRecord {
        public final short category;
        public final short type;
        public final int value;

        private AchievementRecord(short category, short type, int value) {
            this.category = category;
            this.type = type;
            this.value = value;
        }
    }

    private final Path dbPath;
    private final String jdbcUrl;

    public NordDatabase(String path) throws IOException {
        this.dbPath = Paths.get(path);
        this.jdbcUrl = "jdbc:sqlite:" + dbPath.toAbsolutePath();
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException ignored) {
            // Driver is loaded via ServiceLoader when sqlite-jdbc is on the classpath.
        }
        ensureSqliteDriverPresent();
        initSchema();
    }

    public synchronized int getUserCount() {
        String sql = "SELECT COUNT(*) FROM users";
        try (Connection connection = openConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            return rs.next() ? rs.getInt(1) : 0;
        } catch (SQLException e) {
            return 0;
        }
    }

    public synchronized Collection<UserRecord> getAllUsers() {
        ArrayList<UserRecord> users = new ArrayList<>();
        String sql = "SELECT user_id, username, password_hash, village_id, village_name, level, heightmap_preset_id, slx_credits FROM users";
        try (Connection connection = openConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                users.add(mapUser(rs));
            }
        } catch (SQLException ignored) {
            // Return what we could read.
        }
        return Collections.unmodifiableCollection(users);
    }

    public synchronized UserRecord findByUserId(int userId) {
        String sql = "SELECT user_id, username, password_hash, village_id, village_name, level, heightmap_preset_id, slx_credits FROM users WHERE user_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, userId);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? mapUser(rs) : null;
            }
        } catch (SQLException e) {
            return null;
        }
    }

    public synchronized UserRecord findByVillageId(int villageId) {
        String sql = "SELECT user_id, username, password_hash, village_id, village_name, level, heightmap_preset_id, slx_credits FROM users WHERE village_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, villageId);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? mapUser(rs) : null;
            }
        } catch (SQLException e) {
            return null;
        }
    }

    public synchronized UserRecord findByUsername(String username) {
        String normalized = normalizeUsername(username);
        if (normalized.isEmpty()) {
            return null;
        }
        String sql = "SELECT user_id, username, password_hash, village_id, village_name, level, heightmap_preset_id, slx_credits FROM users WHERE lower(username) = lower(?)";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, normalized);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? mapUser(rs) : null;
            }
        } catch (SQLException e) {
            return null;
        }
    }

    public synchronized UserRecord authenticate(String username, String password) {
        UserRecord user = findByUsername(username);
        if (user == null) {
            return null;
        }
        if (verifyPasswordHash(user.username, password, user.passwordHash)) {
            if (isLegacyPasswordHash(user.passwordHash)) {
                try {
                    updatePasswordHash(user.userId, hashPassword(password));
                    UserRecord refreshed = findByUserId(user.userId);
                    return refreshed != null ? refreshed : user;
                } catch (IOException ignored) {
                    // Keep login successful even if hash upgrade fails this time.
                }
            }
            return user;
        }
        return null;
    }

    public synchronized CreateUserResult createUser(String username, String password) throws IOException {
        String normalized = normalizeUsername(username);
        if (normalized.isEmpty()) {
            return new CreateUserResult(false, false, null);
        }
        if (findByUsername(normalized) != null) {
            return new CreateUserResult(false, true, null);
        }

        try (Connection connection = openConnection()) {
            connection.setAutoCommit(false);
            try {
                int userId = nextCounterValue(connection, "next_user_id", 1000);
                int villageId = nextCounterValue(connection, "next_village_id", 2000);
                String villageName = normalized + "'s Village";
                int heightmapPresetId = ((userId - 1000) % HEIGHTMAP_PRESET_COUNT) + 1;
                int startingCredits = 5000;
                String hashedPassword = hashPassword(password);

                String insertSql =
                    "INSERT INTO users (user_id, username, password_hash, village_id, village_name, level, heightmap_preset_id, slx_credits) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
                try (PreparedStatement statement = connection.prepareStatement(insertSql)) {
                    statement.setInt(1, userId);
                    statement.setString(2, normalized);
                    statement.setString(3, hashedPassword);
                    statement.setInt(4, villageId);
                    statement.setString(5, villageName);
                    statement.setInt(6, 5);
                    statement.setInt(7, heightmapPresetId);
                    statement.setInt(8, startingCredits);
                    statement.executeUpdate();
                }

                connection.commit();
                return new CreateUserResult(true, false, new UserRecord(userId, normalized, hashedPassword, villageId, villageName, 5, heightmapPresetId, startingCredits));
            } catch (SQLException e) {
                connection.rollback();
                if (isConstraintViolation(e)) {
                    return new CreateUserResult(false, true, null);
                }
                throw new IOException("Failed to create user", e);
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new IOException("Failed to open database", e);
        }
    }

    public synchronized UserRecord findOrCreateSocialUser(String username) throws IOException {
        UserRecord existing = findByUsername(username);
        if (existing != null) {
            return existing;
        }
        CreateUserResult created = createUser(username, generateRandomSocialPassword());
        return created.user;
    }

    public synchronized ArrayList<Building> loadBuildings(int villageId) {
        ArrayList<Building> result = new ArrayList<>();
        String sql =
            "SELECT building_id, building_type, remaining_points, tile_x, tile_z, parameters, rotation, consumed, placed_on_map, ready, delay_start, delay_end, ingredients " +
            "FROM buildings WHERE village_id = ? ORDER BY building_id";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, villageId);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    result.add(new Building(
                        rs.getShort("building_id"),
                        rs.getShort("building_type"),
                        rs.getFloat("remaining_points"),
                        (byte) rs.getInt("tile_x"),
                        (byte) rs.getInt("tile_z"),
                        nullableBytes(rs.getBytes("parameters")),
                        (byte) rs.getInt("rotation"),
                        rs.getInt("consumed") != 0,
                        rs.getInt("placed_on_map") != 0,
                        rs.getInt("ready") != 0,
                        rs.getLong("delay_start"),
                        rs.getLong("delay_end"),
                        rs.getLong("ingredients")
                    ));
                }
            }
        } catch (SQLException ignored) {
            // Keep empty fallback.
        }
        return result;
    }

    public synchronized void upsertBuilding(int villageId, Building building) throws IOException {
        if (building == null) {
            return;
        }
        String sql =
            "INSERT INTO buildings (village_id, building_id, building_type, remaining_points, tile_x, tile_z, parameters, rotation, consumed, placed_on_map, ready, delay_start, delay_end, ingredients) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT(village_id, building_id) DO UPDATE SET " +
            "building_type=excluded.building_type, remaining_points=excluded.remaining_points, tile_x=excluded.tile_x, tile_z=excluded.tile_z, " +
            "parameters=excluded.parameters, rotation=excluded.rotation, consumed=excluded.consumed, placed_on_map=excluded.placed_on_map, " +
            "ready=excluded.ready, delay_start=excluded.delay_start, delay_end=excluded.delay_end, ingredients=excluded.ingredients";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, villageId);
            statement.setShort(2, building.getBuildingID());
            statement.setShort(3, building.getBuildingType());
            statement.setFloat(4, building.getRemainingPoints());
            statement.setInt(5, building.getTileX());
            statement.setInt(6, building.getTileZ());
            statement.setBytes(7, nullableBytes(building.getParameters()));
            statement.setInt(8, building.getRotation());
            statement.setInt(9, building.isConsumed() ? 1 : 0);
            statement.setInt(10, building.isPlacedOnMap() ? 1 : 0);
            statement.setInt(11, building.isReady() ? 1 : 0);
            statement.setLong(12, building.getDelayStart());
            statement.setLong(13, building.getDelayEnd());
            statement.setLong(14, building.getIngredients());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to upsert building", e);
        }
    }

    public synchronized void applyBuildingUpdate(int villageId, short buildingId, byte[] data, boolean consumed,
                                                 boolean placedOnMap, byte tileX, byte tileZ, byte rotation,
                                                 boolean ready, long delayStart, long delayEnd, long ingredients) throws IOException {
        String sql =
            "UPDATE buildings SET parameters = ?, consumed = ?, placed_on_map = ?, tile_x = ?, tile_z = ?, rotation = ?, ready = ?, delay_start = ?, delay_end = ?, ingredients = ? " +
            "WHERE village_id = ? AND building_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setBytes(1, nullableBytes(data));
            statement.setInt(2, consumed ? 1 : 0);
            statement.setInt(3, placedOnMap ? 1 : 0);
            statement.setInt(4, tileX);
            statement.setInt(5, tileZ);
            statement.setInt(6, rotation);
            statement.setInt(7, ready ? 1 : 0);
            statement.setLong(8, delayStart);
            statement.setLong(9, delayEnd);
            statement.setLong(10, ingredients);
            statement.setInt(11, villageId);
            statement.setShort(12, buildingId);
            int updated = statement.executeUpdate();
            if (updated == 0) {
                Building fallback = new Building(buildingId, (short) 0, 0f, tileX, tileZ, nullableBytes(data), rotation,
                    consumed, placedOnMap, ready, delayStart, delayEnd, ingredients);
                upsertBuilding(villageId, fallback);
            }
        } catch (SQLException e) {
            throw new IOException("Failed to update building", e);
        }
    }

    public synchronized void removeBuilding(int villageId, short buildingId) throws IOException {
        String sql = "DELETE FROM buildings WHERE village_id = ? AND building_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, villageId);
            statement.setShort(2, buildingId);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to remove building", e);
        }
    }

    public synchronized ArrayList<Tree> loadTrees(int villageId) {
        ArrayList<Tree> result = new ArrayList<>();
        String sql = "SELECT tree_id, tree_batch, tree_type, tile_x, tile_z, scale FROM trees WHERE village_id = ? ORDER BY tree_id";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, villageId);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    result.add(new Tree(
                        rs.getShort("tree_id"),
                        rs.getShort("tree_batch"),
                        rs.getShort("tree_type"),
                        (byte) rs.getInt("tile_x"),
                        (byte) rs.getInt("tile_z"),
                        (byte) rs.getInt("scale")
                    ));
                }
            }
        } catch (SQLException ignored) {
            // Keep empty fallback.
        }
        return result;
    }

    public synchronized void upsertTrees(int villageId, ArrayList trees) throws IOException {
        if (trees == null || trees.isEmpty()) {
            return;
        }
        String sql =
            "INSERT INTO trees (village_id, tree_id, tree_batch, tree_type, tile_x, tile_z, scale) VALUES (?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT(village_id, tree_id) DO UPDATE SET " +
            "tree_batch=excluded.tree_batch, tree_type=excluded.tree_type, tile_x=excluded.tile_x, tile_z=excluded.tile_z, scale=excluded.scale";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            for (Object object : trees) {
                if (!(object instanceof Tree)) {
                    continue;
                }
                Tree tree = (Tree) object;
                statement.setInt(1, villageId);
                statement.setShort(2, tree.getTreeID());
                statement.setShort(3, tree.getTreeBatch());
                statement.setShort(4, tree.getTreeType());
                statement.setInt(5, tree.getTileX());
                statement.setInt(6, tree.getTileZ());
                statement.setInt(7, tree.getScale());
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (SQLException e) {
            throw new IOException("Failed to store trees", e);
        }
    }

    public synchronized void removeTrees(int villageId, ArrayList trees) throws IOException {
        if (trees == null || trees.isEmpty()) {
            return;
        }
        String sql = "DELETE FROM trees WHERE village_id = ? AND tree_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            for (Object object : trees) {
                if (!(object instanceof Tree)) {
                    continue;
                }
                Tree tree = (Tree) object;
                statement.setInt(1, villageId);
                statement.setShort(2, tree.getTreeID());
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (SQLException e) {
            throw new IOException("Failed to remove trees", e);
        }
    }

    public synchronized AvatarDetails loadAvatarDetails(int playerId, String fallbackName, int fallbackLevel) {
        UserRecord user = findByUserId(playerId);
        if (user == null) {
            return new AvatarDetails(playerId, fallbackName, (short) 0, (short) 0, new byte[0], "", "", fallbackLevel);
        }

        String sql = "SELECT avatar_data, mount_type, mount_name, pet_type, pet_name FROM avatar_state WHERE player_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    byte[] avatarData = nullableBytes(rs.getBytes("avatar_data"));
                    short mountType = (short) rs.getInt("mount_type");
                    short petType = (short) rs.getInt("pet_type");
                    String mountName = safe(rs.getString("mount_name"));
                    String petName = safe(rs.getString("pet_name"));
                    return new AvatarDetails(user.userId, user.username, mountType, petType, avatarData, mountName, petName, user.level);
                }
            }
        } catch (SQLException ignored) {
            // Return fallback details.
        }
        return new AvatarDetails(user.userId, user.username, (short) 0, (short) 0, new byte[0], "", "", user.level);
    }

    public synchronized void storeAvatarData(int playerId, byte[] avatarData) throws IOException {
        upsertAvatarState(playerId, nullableBytes(avatarData), null, null, null, null);
    }

    public synchronized void storeMount(int playerId, short mountType, String mountName) throws IOException {
        upsertAvatarState(playerId, null, mountType, safe(mountName), null, null);
    }

    public synchronized void storePet(int playerId, short petType, String petName) throws IOException {
        upsertAvatarState(playerId, null, null, null, petType, safe(petName));
    }

    private void initSchema() throws IOException {
        try (Connection connection = openConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA journal_mode=WAL");
            statement.execute("PRAGMA foreign_keys=ON");

            statement.execute(
                "CREATE TABLE IF NOT EXISTS counters (" +
                "name TEXT PRIMARY KEY, " +
                "value INTEGER NOT NULL" +
                ")"
            );
            statement.execute("INSERT OR IGNORE INTO counters(name, value) VALUES ('next_user_id', 1000)");
            statement.execute("INSERT OR IGNORE INTO counters(name, value) VALUES ('next_village_id', 2000)");

            statement.execute(
                "CREATE TABLE IF NOT EXISTS users (" +
                "user_id INTEGER PRIMARY KEY, " +
                "username TEXT NOT NULL UNIQUE COLLATE NOCASE, " +
                "password_hash TEXT NOT NULL, " +
                "village_id INTEGER NOT NULL UNIQUE, " +
                "village_name TEXT NOT NULL, " +
                "level INTEGER NOT NULL DEFAULT 1, " +
                "heightmap_preset_id INTEGER NOT NULL DEFAULT 0, " +
                "slx_credits INTEGER NOT NULL DEFAULT 5000" +
                ")"
            );
            try {
                statement.execute("ALTER TABLE users ADD COLUMN heightmap_preset_id INTEGER NOT NULL DEFAULT 0");
            } catch (SQLException ignored) {
                // Column already exists on upgraded databases.
            }
            try {
                statement.execute("ALTER TABLE users ADD COLUMN slx_credits INTEGER NOT NULL DEFAULT 5000");
            } catch (SQLException ignored) {
                // Column already exists on upgraded databases.
            }

            // One-time migration: keep existing villages on legacy terrain.
            statement.execute("INSERT OR IGNORE INTO counters(name, value) VALUES ('heightmap_preset_backfill_done', 0)");
            int backfillDone = 0;
            try (ResultSet rs = statement.executeQuery("SELECT value FROM counters WHERE name = 'heightmap_preset_backfill_done'")) {
                if (rs.next()) {
                    backfillDone = rs.getInt(1);
                }
            }
            if (backfillDone == 0) {
                statement.executeUpdate("UPDATE users SET heightmap_preset_id = 0");
                statement.executeUpdate("UPDATE counters SET value = 1 WHERE name = 'heightmap_preset_backfill_done'");
            }

            statement.execute(
                "CREATE TABLE IF NOT EXISTS buildings (" +
                "village_id INTEGER NOT NULL, " +
                "building_id INTEGER NOT NULL, " +
                "building_type INTEGER NOT NULL, " +
                "remaining_points REAL NOT NULL DEFAULT 0, " +
                "tile_x INTEGER NOT NULL DEFAULT 0, " +
                "tile_z INTEGER NOT NULL DEFAULT 0, " +
                "parameters BLOB, " +
                "rotation INTEGER NOT NULL DEFAULT 0, " +
                "consumed INTEGER NOT NULL DEFAULT 0, " +
                "placed_on_map INTEGER NOT NULL DEFAULT 0, " +
                "ready INTEGER NOT NULL DEFAULT 0, " +
                "delay_start INTEGER NOT NULL DEFAULT 0, " +
                "delay_end INTEGER NOT NULL DEFAULT 0, " +
                "ingredients INTEGER NOT NULL DEFAULT 0, " +
                "PRIMARY KEY(village_id, building_id)" +
                ")"
            );

            statement.execute(
                "CREATE TABLE IF NOT EXISTS trees (" +
                "village_id INTEGER NOT NULL, " +
                "tree_id INTEGER NOT NULL, " +
                "tree_batch INTEGER NOT NULL, " +
                "tree_type INTEGER NOT NULL, " +
                "tile_x INTEGER NOT NULL, " +
                "tile_z INTEGER NOT NULL, " +
                "scale INTEGER NOT NULL, " +
                "PRIMARY KEY(village_id, tree_id)" +
                ")"
            );

            statement.execute(
                "CREATE TABLE IF NOT EXISTS avatar_state (" +
                "player_id INTEGER PRIMARY KEY, " +
                "avatar_data BLOB, " +
                "mount_type INTEGER NOT NULL DEFAULT 0, " +
                "mount_name TEXT NOT NULL DEFAULT '', " +
                "pet_type INTEGER NOT NULL DEFAULT 0, " +
                "pet_name TEXT NOT NULL DEFAULT ''" +
                ")"
            );

            statement.execute(
                "CREATE TABLE IF NOT EXISTS spin_state (" +
                "player_id INTEGER PRIMARY KEY, " +
                "level INTEGER NOT NULL DEFAULT 1, " +
                "wins INTEGER NOT NULL DEFAULT 0, " +
                "won INTEGER NOT NULL DEFAULT 0, " +
                "lit1 INTEGER NOT NULL DEFAULT 0, " +
                "lit2 INTEGER NOT NULL DEFAULT 0, " +
                "lit3 INTEGER NOT NULL DEFAULT 0, " +
                "lit4 INTEGER NOT NULL DEFAULT 0, " +
                "lit5 INTEGER NOT NULL DEFAULT 0, " +
                "lit6 INTEGER NOT NULL DEFAULT 0, " +
                "lit7 INTEGER NOT NULL DEFAULT 0, " +
                "lit8 INTEGER NOT NULL DEFAULT 0, " +
                "res1 INTEGER NOT NULL DEFAULT 0, " +
                "res2 INTEGER NOT NULL DEFAULT 0, " +
                "res3 INTEGER NOT NULL DEFAULT 0, " +
                "res4 INTEGER NOT NULL DEFAULT 0, " +
                "res5 INTEGER NOT NULL DEFAULT 0, " +
                "res6 INTEGER NOT NULL DEFAULT 0, " +
                "res7 INTEGER NOT NULL DEFAULT 0, " +
                "res8 INTEGER NOT NULL DEFAULT 0, " +
                "updated_at INTEGER NOT NULL DEFAULT 0" +
                ")"
            );

            statement.execute(
                "CREATE TABLE IF NOT EXISTS achievements (" +
                "player_id INTEGER NOT NULL, " +
                "category INTEGER NOT NULL, " +
                "type INTEGER NOT NULL, " +
                "value INTEGER NOT NULL DEFAULT 0, " +
                "updated_at INTEGER NOT NULL DEFAULT 0, " +
                "PRIMARY KEY(player_id, category, type)" +
                ")"
            );
        } catch (SQLException e) {
            if (e.getMessage() != null && e.getMessage().toLowerCase().contains("no suitable driver")) {
                throw new IOException(
                    "Failed to initialize schema: SQLite JDBC driver not found. Add sqlite-jdbc-<version>.jar to the project root so launch scripts include it.",
                    e
                );
            }
            throw new IOException("Failed to initialize schema", e);
        }
    }

    private Connection openConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    private void ensureSqliteDriverPresent() throws IOException {
        try {
            Driver driver = DriverManager.getDriver("jdbc:sqlite:");
            if (driver == null) {
                throw new IOException("SQLite JDBC driver unavailable.");
            }
        } catch (SQLException e) {
            throw new IOException(
                "SQLite JDBC driver not found. Add a sqlite JDBC jar (for example sqlite-jdbc-<version>.jar) to the project root so it is included in the classpath.",
                e
            );
        }
    }

    private static String normalizeUsername(String username) {
        return username == null ? "" : username.trim();
    }

    private static String safe(String value) {
        return value == null ? "" : value;
    }

    private static byte[] nullableBytes(byte[] data) {
        return data == null ? new byte[0] : data;
    }

    private UserRecord mapUser(ResultSet rs) throws SQLException {
        return new UserRecord(
            rs.getInt("user_id"),
            rs.getString("username"),
            rs.getString("password_hash"),
            rs.getInt("village_id"),
            rs.getString("village_name"),
            rs.getInt("level"),
            rs.getInt("heightmap_preset_id"),
            rs.getInt("slx_credits")
        );
    }

    public synchronized int getSLXCredits(int userId) {
        String sql = "SELECT slx_credits FROM users WHERE user_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, userId);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        } catch (SQLException e) {
            return 0;
        }
    }

    public synchronized int spendSLXCredits(int userId, int amount) throws IOException {
        int safeAmount = Math.max(0, amount);
        String sql = "UPDATE users SET slx_credits = CASE WHEN slx_credits >= ? THEN slx_credits - ? ELSE slx_credits END WHERE user_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, safeAmount);
            statement.setInt(2, safeAmount);
            statement.setInt(3, userId);
            statement.executeUpdate();
            return getSLXCredits(userId);
        } catch (SQLException e) {
            throw new IOException("Failed to spend SLX credits", e);
        }
    }

    public synchronized int addSLXCredits(int userId, int amount) throws IOException {
        int safeAmount = Math.max(0, amount);
        String sql = "UPDATE users SET slx_credits = slx_credits + ? WHERE user_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, safeAmount);
            statement.setInt(2, userId);
            statement.executeUpdate();
            return getSLXCredits(userId);
        } catch (SQLException e) {
            throw new IOException("Failed to add SLX credits", e);
        }
    }

    public synchronized int applySLXCreditDelta(int userId, int delta) throws IOException {
        if (delta == 0) {
            return getSLXCredits(userId);
        }
        String sql = "UPDATE users SET slx_credits = MAX(0, slx_credits + ?) WHERE user_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, delta);
            statement.setInt(2, userId);
            statement.executeUpdate();
            return getSLXCredits(userId);
        } catch (SQLException e) {
            throw new IOException("Failed to apply SLX credits delta", e);
        }
    }

    public synchronized int updateUserLevel(int userId, int level) throws IOException {
        int nextLevel = Math.max(1, level);
        String sql = "UPDATE users SET level = CASE WHEN level < ? THEN ? ELSE level END WHERE user_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, nextLevel);
            statement.setInt(2, nextLevel);
            statement.setInt(3, userId);
            statement.executeUpdate();
            UserRecord user = findByUserId(userId);
            return user != null ? user.level : nextLevel;
        } catch (SQLException e) {
            throw new IOException("Failed to update user level", e);
        }
    }

    public synchronized void storeSpinState(int playerId,
                                            short level,
                                            short wins,
                                            boolean won,
                                            boolean lit1, boolean lit2, boolean lit3, boolean lit4,
                                            boolean lit5, boolean lit6, boolean lit7, boolean lit8,
                                            short res1, short res2, short res3, short res4,
                                            short res5, short res6, short res7, short res8) throws IOException {
        String sql =
            "INSERT INTO spin_state (" +
            "player_id, level, wins, won, " +
            "lit1, lit2, lit3, lit4, lit5, lit6, lit7, lit8, " +
            "res1, res2, res3, res4, res5, res6, res7, res8, updated_at" +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT(player_id) DO UPDATE SET " +
            "level=excluded.level, wins=excluded.wins, won=excluded.won, " +
            "lit1=excluded.lit1, lit2=excluded.lit2, lit3=excluded.lit3, lit4=excluded.lit4, " +
            "lit5=excluded.lit5, lit6=excluded.lit6, lit7=excluded.lit7, lit8=excluded.lit8, " +
            "res1=excluded.res1, res2=excluded.res2, res3=excluded.res3, res4=excluded.res4, " +
            "res5=excluded.res5, res6=excluded.res6, res7=excluded.res7, res8=excluded.res8, " +
            "updated_at=excluded.updated_at";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            statement.setInt(2, Math.max(1, level));
            statement.setInt(3, Math.max(0, wins));
            statement.setInt(4, won ? 1 : 0);
            statement.setInt(5, lit1 ? 1 : 0);
            statement.setInt(6, lit2 ? 1 : 0);
            statement.setInt(7, lit3 ? 1 : 0);
            statement.setInt(8, lit4 ? 1 : 0);
            statement.setInt(9, lit5 ? 1 : 0);
            statement.setInt(10, lit6 ? 1 : 0);
            statement.setInt(11, lit7 ? 1 : 0);
            statement.setInt(12, lit8 ? 1 : 0);
            statement.setInt(13, res1);
            statement.setInt(14, res2);
            statement.setInt(15, res3);
            statement.setInt(16, res4);
            statement.setInt(17, res5);
            statement.setInt(18, res6);
            statement.setInt(19, res7);
            statement.setInt(20, res8);
            statement.setLong(21, System.currentTimeMillis());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to store spin state", e);
        }
    }

    public synchronized int applyAchievementDelta(int playerId, short category, short type, int changeAmount, boolean add) throws IOException {
        String selectSql = "SELECT value FROM achievements WHERE player_id = ? AND category = ? AND type = ?";
        String upsertSql =
            "INSERT INTO achievements (player_id, category, type, value, updated_at) VALUES (?, ?, ?, ?, ?) " +
            "ON CONFLICT(player_id, category, type) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at";
        try (Connection connection = openConnection()) {
            int current = 0;
            try (PreparedStatement select = connection.prepareStatement(selectSql)) {
                select.setInt(1, playerId);
                select.setInt(2, category);
                select.setInt(3, type);
                try (ResultSet rs = select.executeQuery()) {
                    if (rs.next()) {
                        current = rs.getInt(1);
                    }
                }
            }
            int next = add ? (current + changeAmount) : changeAmount;
            if (next < 0) {
                next = 0;
            }
            try (PreparedStatement upsert = connection.prepareStatement(upsertSql)) {
                upsert.setInt(1, playerId);
                upsert.setInt(2, category);
                upsert.setInt(3, type);
                upsert.setInt(4, next);
                upsert.setLong(5, System.currentTimeMillis());
                upsert.executeUpdate();
            }
            return next;
        } catch (SQLException e) {
            throw new IOException("Failed to apply achievement delta", e);
        }
    }

    public synchronized ArrayList<AchievementRecord> loadAchievements(int playerId) {
        ArrayList<AchievementRecord> achievements = new ArrayList<>();
        String sql = "SELECT category, type, value FROM achievements WHERE player_id = ? ORDER BY category, type";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    achievements.add(new AchievementRecord(
                        (short) rs.getInt("category"),
                        (short) rs.getInt("type"),
                        rs.getInt("value")
                    ));
                }
            }
        } catch (SQLException ignored) {
            // Keep empty fallback.
        }
        return achievements;
    }

    private int nextCounterValue(Connection connection, String name, int defaultValue) throws SQLException {
        int current = defaultValue;
        String select = "SELECT value FROM counters WHERE name = ?";
        try (PreparedStatement statement = connection.prepareStatement(select)) {
            statement.setString(1, name);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    current = rs.getInt(1);
                } else {
                    try (PreparedStatement insert = connection.prepareStatement("INSERT INTO counters(name, value) VALUES(?, ?)")) {
                        insert.setString(1, name);
                        insert.setInt(2, defaultValue);
                        insert.executeUpdate();
                    }
                    current = defaultValue;
                }
            }
        }

        try (PreparedStatement update = connection.prepareStatement("UPDATE counters SET value = ? WHERE name = ?")) {
            update.setInt(1, current + 1);
            update.setString(2, name);
            update.executeUpdate();
        }
        return current;
    }

    private void upsertAvatarState(int playerId, byte[] avatarData, Short mountType, String mountName, Short petType, String petName) throws IOException {
        String sql =
            "INSERT INTO avatar_state (player_id, avatar_data, mount_type, mount_name, pet_type, pet_name) VALUES (?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT(player_id) DO UPDATE SET " +
            "avatar_data = COALESCE(excluded.avatar_data, avatar_state.avatar_data), " +
            "mount_type = CASE WHEN excluded.mount_type IS NULL THEN avatar_state.mount_type ELSE excluded.mount_type END, " +
            "mount_name = CASE WHEN excluded.mount_name IS NULL THEN avatar_state.mount_name ELSE excluded.mount_name END, " +
            "pet_type = CASE WHEN excluded.pet_type IS NULL THEN avatar_state.pet_type ELSE excluded.pet_type END, " +
            "pet_name = CASE WHEN excluded.pet_name IS NULL THEN avatar_state.pet_name ELSE excluded.pet_name END";

        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            if (avatarData == null) {
                statement.setNull(2, java.sql.Types.BLOB);
            } else {
                statement.setBytes(2, avatarData);
            }
            if (mountType == null) {
                statement.setNull(3, java.sql.Types.INTEGER);
            } else {
                statement.setInt(3, mountType);
            }
            if (mountName == null) {
                statement.setNull(4, java.sql.Types.VARCHAR);
            } else {
                statement.setString(4, mountName);
            }
            if (petType == null) {
                statement.setNull(5, java.sql.Types.INTEGER);
            } else {
                statement.setInt(5, petType);
            }
            if (petName == null) {
                statement.setNull(6, java.sql.Types.VARCHAR);
            } else {
                statement.setString(6, petName);
            }
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to store avatar state", e);
        }
    }

    private static boolean isConstraintViolation(SQLException e) {
        String state = e.getSQLState();
        if (state != null && state.startsWith("23")) {
            return true;
        }
        String message = e.getMessage();
        return message != null && message.toLowerCase().contains("constraint");
    }

    private synchronized void updatePasswordHash(int userId, String newHash) throws IOException {
        String sql = "UPDATE users SET password_hash = ? WHERE user_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, newHash);
            statement.setInt(2, userId);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to update password hash", e);
        }
    }

    private static boolean verifyPasswordHash(String username, String password, String storedHash) {
        if (storedHash == null || storedHash.trim().isEmpty()) {
            return false;
        }
        if (storedHash.startsWith("pbkdf2$")) {
            try {
                String[] parts = storedHash.split("\\$");
                if (parts.length != 4) {
                    return false;
                }
                int iterations = Integer.parseInt(parts[1]);
                byte[] salt = Base64.getDecoder().decode(parts[2]);
                byte[] expected = Base64.getDecoder().decode(parts[3]);
                byte[] actual = derivePbkdf2(password, salt, iterations, expected.length);
                return MessageDigest.isEqual(actual, expected);
            } catch (Exception ignored) {
                return false;
            }
        }
        String incoming = passwordHashLegacy(username, password);
        return MessageDigest.isEqual(
            incoming.getBytes(java.nio.charset.StandardCharsets.UTF_8),
            storedHash.getBytes(java.nio.charset.StandardCharsets.UTF_8)
        );
    }

    private static boolean isLegacyPasswordHash(String storedHash) {
        return storedHash != null && storedHash.matches("^[0-9a-fA-F]{64}$");
    }

    private static String hashPassword(String password) {
        byte[] salt = new byte[PASSWORD_SALT_BYTES];
        PASSWORD_RANDOM.nextBytes(salt);
        byte[] derived = derivePbkdf2(password, salt, PASSWORD_PBKDF2_ITERATIONS, PASSWORD_HASH_BYTES);
        return "pbkdf2$" + PASSWORD_PBKDF2_ITERATIONS + "$" +
            Base64.getEncoder().encodeToString(salt) + "$" +
            Base64.getEncoder().encodeToString(derived);
    }

    private static byte[] derivePbkdf2(String password, byte[] salt, int iterations, int keyBytes) {
        try {
            PBEKeySpec spec = new PBEKeySpec(
                (password == null ? "" : password).toCharArray(),
                salt,
                iterations,
                keyBytes * 8
            );
            try {
                SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
                return factory.generateSecret(spec).getEncoded();
            } catch (NoSuchAlgorithmException ignored) {
                SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
                return factory.generateSecret(spec).getEncoded();
            } finally {
                spec.clearPassword();
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to derive password hash", e);
        }
    }

    private static String passwordHashLegacy(String username, String password) {
        String seed = (username == null ? "" : username.toLowerCase()) + ":" + (password == null ? "" : password);
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(seed.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static String generateRandomSocialPassword() {
        byte[] bytes = new byte[24];
        PASSWORD_RANDOM.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }
}
