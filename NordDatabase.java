import com.slx.nord.jgnpersistentobjectdetails.AvatarDetails;
import com.slx.nord.jgnpersistentobjectdetails.Building;
import com.slx.nord.jgnpersistentobjectdetails.Clothes;
import com.slx.nord.jgnpersistentobjectdetails.Event;
import com.slx.nord.jgnpersistentobjectdetails.SimulatedCharacterData;
import com.slx.nord.jgnpersistentobjectdetails.Tree;

import java.io.IOException;
import java.nio.file.Files;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.time.LocalDate;
import java.time.Period;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

public class NordDatabase {
    private static final int HEIGHTMAP_PRESET_COUNT = 50;
    private static final int DEFAULT_VILLAGE_THEME = 0;
    private static final int DEFAULT_VILLAGE_BOUNDS_SIZE = 6;
    private static final int PASSWORD_PBKDF2_ITERATIONS = 120000;
    private static final int PASSWORD_SALT_BYTES = 16;
    private static final int PASSWORD_HASH_BYTES = 32;
    private static final short ROLE_CATEGORY = 6;
    private static final short ROLE_TYPE_VIP = 0;
    private static final int ENABLED_ROLE_VALUE = 1;
    private static final String CHAT_COMMAND_PERMISSION_EVERYONE = "everyone";
    private static final String CHAT_COMMAND_PERMISSION_VIP = "vip";
    private static final String CHAT_COMMAND_PERMISSION_MOD = "mod";
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

    public static final class ResourcePlacement {
        public final byte tileX;
        public final byte tileZ;
        public final byte rotation;

        public ResourcePlacement(byte tileX, byte tileZ, byte rotation) {
            this.tileX = tileX;
            this.tileZ = tileZ;
            this.rotation = rotation;
        }
    }

    public static final class AccountProfileRecord {
        public final String firstName;
        public final String surName;
        public final String email;
        public final String city;
        public final String presentationPictureUrl;
        public final int countryId;
        public final byte birthDate;
        public final byte birthMonth;
        public final short birthYear;
        public final byte sex;
        public final boolean wantsEmail;
        public final int referralId;
        public final long fbUser;
        public final String accessToken;
        public final long createdAt;

        private AccountProfileRecord(String firstName,
                                     String surName,
                                     String email,
                                     String city,
                                     String presentationPictureUrl,
                                     int countryId,
                                     byte birthDate,
                                     byte birthMonth,
                                     short birthYear,
                                     byte sex,
                                     boolean wantsEmail,
                                     int referralId,
                                     long fbUser,
                                     String accessToken,
                                     long createdAt) {
            this.firstName = firstName;
            this.surName = surName;
            this.email = email;
            this.city = city;
            this.presentationPictureUrl = presentationPictureUrl;
            this.countryId = countryId;
            this.birthDate = birthDate;
            this.birthMonth = birthMonth;
            this.birthYear = birthYear;
            this.sex = sex;
            this.wantsEmail = wantsEmail;
            this.referralId = referralId;
            this.fbUser = fbUser;
            this.accessToken = accessToken;
            this.createdAt = createdAt;
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

    public static final class CompanionOwnershipRecord {
        public final String companionKind;
        public final short companionType;
        public final long acquiredAt;
        public final String source;

        private CompanionOwnershipRecord(String companionKind, short companionType, long acquiredAt, String source) {
            this.companionKind = companionKind;
            this.companionType = companionType;
            this.acquiredAt = acquiredAt;
            this.source = source;
        }
    }

    public static final class WheelResourceStateRecord {
        public final boolean lit1;
        public final boolean lit2;
        public final boolean lit3;
        public final boolean lit4;
        public final boolean lit5;
        public final boolean lit6;
        public final boolean lit7;
        public final boolean lit8;
        public final short res1;
        public final short res2;
        public final short res3;
        public final short res4;
        public final short res5;
        public final short res6;
        public final short res7;
        public final short res8;
        public final long updatedAt;

        private WheelResourceStateRecord(boolean lit1,
                                         boolean lit2,
                                         boolean lit3,
                                         boolean lit4,
                                         boolean lit5,
                                         boolean lit6,
                                         boolean lit7,
                                         boolean lit8,
                                         short res1,
                                         short res2,
                                         short res3,
                                         short res4,
                                         short res5,
                                         short res6,
                                         short res7,
                                         short res8,
                                         long updatedAt) {
            this.lit1 = lit1;
            this.lit2 = lit2;
            this.lit3 = lit3;
            this.lit4 = lit4;
            this.lit5 = lit5;
            this.lit6 = lit6;
            this.lit7 = lit7;
            this.lit8 = lit8;
            this.res1 = res1;
            this.res2 = res2;
            this.res3 = res3;
            this.res4 = res4;
            this.res5 = res5;
            this.res6 = res6;
            this.res7 = res7;
            this.res8 = res8;
            this.updatedAt = updatedAt;
        }
    }

    public static final class SpinStateRecord {
        public final short level;
        public final short wins;
        public final boolean won;
        public final boolean lit1;
        public final boolean lit2;
        public final boolean lit3;
        public final boolean lit4;
        public final boolean lit5;
        public final boolean lit6;
        public final boolean lit7;
        public final boolean lit8;
        public final short res1;
        public final short res2;
        public final short res3;
        public final short res4;
        public final short res5;
        public final short res6;
        public final short res7;
        public final short res8;
        public final long updatedAt;

        private SpinStateRecord(short level,
                                short wins,
                                boolean won,
                                boolean lit1,
                                boolean lit2,
                                boolean lit3,
                                boolean lit4,
                                boolean lit5,
                                boolean lit6,
                                boolean lit7,
                                boolean lit8,
                                short res1,
                                short res2,
                                short res3,
                                short res4,
                                short res5,
                                short res6,
                                short res7,
                                short res8,
                                long updatedAt) {
            this.level = level;
            this.wins = wins;
            this.won = won;
            this.lit1 = lit1;
            this.lit2 = lit2;
            this.lit3 = lit3;
            this.lit4 = lit4;
            this.lit5 = lit5;
            this.lit6 = lit6;
            this.lit7 = lit7;
            this.lit8 = lit8;
            this.res1 = res1;
            this.res2 = res2;
            this.res3 = res3;
            this.res4 = res4;
            this.res5 = res5;
            this.res6 = res6;
            this.res7 = res7;
            this.res8 = res8;
            this.updatedAt = updatedAt;
        }
    }

    public static final class GuestbookEntryRecord {
        public final int postId;
        public final int ownerPlayerId;
        public final int posterPlayerId;
        public final String posterName;
        public final int posterLevel;
        public final String postText;
        public final long postTime;
        public final boolean isPrivate;
        public final String type;

        private GuestbookEntryRecord(int postId,
                                     int ownerPlayerId,
                                     int posterPlayerId,
                                     String posterName,
                                     int posterLevel,
                                     String postText,
                                     long postTime,
                                     boolean isPrivate,
                                     String type) {
            this.postId = postId;
            this.ownerPlayerId = ownerPlayerId;
            this.posterPlayerId = posterPlayerId;
            this.posterName = posterName;
            this.posterLevel = posterLevel;
            this.postText = postText;
            this.postTime = postTime;
            this.isPrivate = isPrivate;
            this.type = type;
        }
    }

    public static final class GuestbookPage {
        public final boolean hasNewerPosts;
        public final boolean hasOlderPosts;
        public final ArrayList<GuestbookEntryRecord> entries;

        private GuestbookPage(boolean hasNewerPosts, boolean hasOlderPosts, ArrayList<GuestbookEntryRecord> entries) {
            this.hasNewerPosts = hasNewerPosts;
            this.hasOlderPosts = hasOlderPosts;
            this.entries = entries;
        }
    }

    public static final class HeightMapRecord {
        public final byte[] heightData;
        public final byte type;
        public final byte size;

        private HeightMapRecord(byte[] heightData, byte type, byte size) {
            this.heightData = nullableBytes(heightData);
            this.type = type;
            this.size = size;
        }
    }

    public static final class AddBuildingPurchaseResult {
        public final boolean added;
        public final boolean insufficientCredits;
        public final int creditsAfter;

        private AddBuildingPurchaseResult(boolean added, boolean insufficientCredits, int creditsAfter) {
            this.added = added;
            this.insufficientCredits = insufficientCredits;
            this.creditsAfter = creditsAfter;
        }
    }

    public static final class RemoveBuildingRefundResult {
        public final boolean removed;
        public final int creditsAfter;
        public final int refundGranted;
        public final short buildingType;

        private RemoveBuildingRefundResult(boolean removed, int creditsAfter, int refundGranted, short buildingType) {
            this.removed = removed;
            this.creditsAfter = creditsAfter;
            this.refundGranted = refundGranted;
            this.buildingType = buildingType;
        }
    }

    public static final class LevelRewardResult {
        public final int previousLevel;
        public final int newLevel;
        public final int creditsAwarded;
        public final int creditsAfter;
        public final int lastRewardedLevel;

        private LevelRewardResult(int previousLevel, int newLevel, int creditsAwarded, int creditsAfter, int lastRewardedLevel) {
            this.previousLevel = previousLevel;
            this.newLevel = newLevel;
            this.creditsAwarded = creditsAwarded;
            this.creditsAfter = creditsAfter;
            this.lastRewardedLevel = lastRewardedLevel;
        }
    }

    public static final class MidiRecord {
        public final long songId;
        public final int playerId;
        public final boolean isGift;
        public final byte[] data;

        private MidiRecord(long songId, int playerId, boolean isGift, byte[] data) {
            this.songId = songId;
            this.playerId = playerId;
            this.isGift = isGift;
            this.data = nullableBytes(data);
        }
    }

    public static final class QuestRecord {
        public final long questId;
        public final int playerId;
        public final int targetPlayerId;
        public final short type;
        public final byte[] data;
        public final boolean finished;

        private QuestRecord(long questId, int playerId, int targetPlayerId, short type, byte[] data, boolean finished) {
            this.questId = questId;
            this.playerId = playerId;
            this.targetPlayerId = targetPlayerId;
            this.type = type;
            this.data = nullableBytes(data);
            this.finished = finished;
        }
    }

    public static final class PointWalkQuestionStatsRecord {
        public final int nrCorrectAnswers;
        public final int nrWrongAnswers;
        public final int nr1s;
        public final int nrXs;
        public final int nr2s;

        private PointWalkQuestionStatsRecord(int nrCorrectAnswers, int nrWrongAnswers, int nr1s, int nrXs, int nr2s) {
            this.nrCorrectAnswers = nrCorrectAnswers;
            this.nrWrongAnswers = nrWrongAnswers;
            this.nr1s = nr1s;
            this.nrXs = nrXs;
            this.nr2s = nr2s;
        }
    }

    public static final class PointWalkHighscoreRecord {
        public final int playerId;
        public final String playerName;
        public final int score;

        private PointWalkHighscoreRecord(int playerId, String playerName, int score) {
            this.playerId = playerId;
            this.playerName = safe(playerName);
            this.score = score;
        }
    }

    public static final class EventPage {
        public final ArrayList<Event> events;
        public final boolean olderEventsAvailable;

        private EventPage(ArrayList<Event> events, boolean olderEventsAvailable) {
            this.events = events;
            this.olderEventsAvailable = olderEventsAvailable;
        }
    }

    public static final class TwitterTokensRecord {
        public final int playerId;
        public final String token;
        public final String tokenSecret;
        public final int sessionId;
        public final long updatedAt;

        private TwitterTokensRecord(int playerId, String token, String tokenSecret, int sessionId, long updatedAt) {
            this.playerId = playerId;
            this.token = safe(token);
            this.tokenSecret = safe(tokenSecret);
            this.sessionId = sessionId;
            this.updatedAt = updatedAt;
        }
    }

    public static final class LoginBlacklistRecord {
        public final String message;
        public final long blockedUntil;

        private LoginBlacklistRecord(String message, long blockedUntil) {
            this.message = safe(message);
            this.blockedUntil = blockedUntil;
        }
    }

    public static final class AdminPopupQueueRecord {
        public final long id;
        public final String senderName;
        public final String messageText;
        public final long createdAt;

        private AdminPopupQueueRecord(long id, String senderName, String messageText, long createdAt) {
            this.id = id;
            this.senderName = safe(senderName);
            this.messageText = safe(messageText);
            this.createdAt = createdAt;
        }
    }

    private final Path dbPath;
    private final String jdbcUrl;
    private volatile Map<Short, Integer> buildingBasePricesByType = Collections.emptyMap();

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
                int startingCredits = 3000;
                int startingLevel = 1;
                String hashedPassword = hashPassword(password);

                String insertSql =
                    "INSERT INTO users (user_id, username, password_hash, village_id, village_name, level, heightmap_preset_id, slx_credits, last_rewarded_level) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
                try (PreparedStatement statement = connection.prepareStatement(insertSql)) {
                    statement.setInt(1, userId);
                    statement.setString(2, normalized);
                    statement.setString(3, hashedPassword);
                    statement.setInt(4, villageId);
                    statement.setString(5, villageName);
                    statement.setInt(6, startingLevel);
                    statement.setInt(7, heightmapPresetId);
                    statement.setInt(8, startingCredits);
                    statement.setInt(9, Math.max(0, startingLevel - 1));
                    statement.executeUpdate();
                }

                String insertVipRoleSql =
                    "INSERT INTO achievements (player_id, category, type, value, updated_at) VALUES (?, ?, ?, ?, ?)";
                try (PreparedStatement statement = connection.prepareStatement(insertVipRoleSql)) {
                    statement.setInt(1, userId);
                    statement.setInt(2, ROLE_CATEGORY);
                    statement.setInt(3, ROLE_TYPE_VIP);
                    statement.setInt(4, ENABLED_ROLE_VALUE);
                    statement.setLong(5, System.currentTimeMillis());
                    statement.executeUpdate();
                }

                connection.commit();
                return new CreateUserResult(true, false, new UserRecord(userId, normalized, hashedPassword, villageId, villageName, startingLevel, heightmapPresetId, startingCredits));
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

    public synchronized void storeAccountProfile(int playerId,
                                                 String firstName,
                                                 String surName,
                                                 String email,
                                                 int countryId,
                                                 byte birthDate,
                                                 byte birthMonth,
                                                 short birthYear,
                                                 byte sex,
                                                 int referralId,
                                                 long fbUser,
                                                 String accessToken) throws IOException {
        String sql =
            "INSERT INTO account_profiles (player_id, first_name, sur_name, email, country_id, birth_date, birth_month, birth_year, sex, referral_id, fb_user, access_token, created_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM account_profiles WHERE player_id = ?), ?)) " +
            "ON CONFLICT(player_id) DO UPDATE SET " +
            "first_name = excluded.first_name, " +
            "sur_name = excluded.sur_name, " +
            "email = excluded.email, " +
            "country_id = excluded.country_id, " +
            "birth_date = excluded.birth_date, " +
            "birth_month = excluded.birth_month, " +
            "birth_year = excluded.birth_year, " +
            "sex = excluded.sex, " +
            "referral_id = excluded.referral_id, " +
            "fb_user = excluded.fb_user, " +
            "access_token = excluded.access_token";
        long now = System.currentTimeMillis();
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            statement.setString(2, safe(firstName));
            statement.setString(3, safe(surName));
            statement.setString(4, safe(email));
            statement.setInt(5, countryId);
            statement.setInt(6, birthDate);
            statement.setInt(7, birthMonth);
            statement.setInt(8, birthYear);
            statement.setInt(9, sex);
            statement.setInt(10, referralId);
            statement.setLong(11, fbUser);
            statement.setString(12, safe(accessToken));
            statement.setInt(13, playerId);
            statement.setLong(14, now);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to store account profile", e);
        }
    }

    public synchronized AccountProfileRecord loadAccountProfile(int playerId) {
        String sql =
            "SELECT first_name, sur_name, email, city, presentation_picture_url, country_id, birth_date, birth_month, birth_year, sex, wants_email, referral_id, fb_user, access_token, created_at " +
            "FROM account_profiles WHERE player_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new AccountProfileRecord(
                    safe(rs.getString("first_name")),
                    safe(rs.getString("sur_name")),
                    safe(rs.getString("email")),
                    safe(rs.getString("city")),
                    safe(rs.getString("presentation_picture_url")),
                    rs.getInt("country_id"),
                    (byte) rs.getInt("birth_date"),
                    (byte) rs.getInt("birth_month"),
                    (short) rs.getInt("birth_year"),
                    (byte) rs.getInt("sex"),
                    rs.getInt("wants_email") != 0,
                    rs.getInt("referral_id"),
                    rs.getLong("fb_user"),
                    safe(rs.getString("access_token")),
                    rs.getLong("created_at")
                );
            }
        } catch (SQLException ignored) {
            return null;
        }
    }

    public synchronized void storePlayerStatus(int playerId, String statusText) throws IOException {
        String sql =
            "INSERT INTO account_profiles (player_id, city, created_at) " +
            "VALUES (?, ?, COALESCE((SELECT created_at FROM account_profiles WHERE player_id = ?), ?)) " +
            "ON CONFLICT(player_id) DO UPDATE SET city = excluded.city";
        long now = System.currentTimeMillis();
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            statement.setString(2, safe(statusText));
            statement.setInt(3, playerId);
            statement.setLong(4, now);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to store player status", e);
        }
    }

    public synchronized void storePlayerInformation(int playerId,
                                                    String firstName,
                                                    String surName,
                                                    Short birthYear,
                                                    Byte birthMonth,
                                                    Byte birthDate,
                                                    String presentationPictureUrl,
                                                    Short countryId,
                                                    String city,
                                                    Boolean wantsEmail) throws IOException {
        if (playerId <= 0) {
            return;
        }

        AccountProfileRecord existing = loadAccountProfile(playerId);
        String resolvedFirstName = firstName != null
            ? safe(firstName)
            : (existing != null ? safe(existing.firstName) : "");
        String resolvedSurName = surName != null
            ? safe(surName)
            : (existing != null ? safe(existing.surName) : "");
        String resolvedEmail = existing != null ? safe(existing.email) : "";
        String resolvedCity = city != null
            ? safe(city)
            : (existing != null ? safe(existing.city) : "");
        String resolvedPresentationPictureUrl = presentationPictureUrl != null
            ? safe(presentationPictureUrl)
            : (existing != null ? safe(existing.presentationPictureUrl) : "");
        int resolvedCountryId = countryId != null
            ? Math.max(0, countryId.intValue())
            : (existing != null ? Math.max(0, existing.countryId) : 0);
        int resolvedBirthDate = birthDate != null
            ? Math.max(0, birthDate.intValue())
            : (existing != null ? Math.max(0, existing.birthDate) : 0);
        int resolvedBirthMonth = birthMonth != null
            ? Math.max(0, birthMonth.intValue())
            : (existing != null ? Math.max(0, existing.birthMonth) : 0);
        int resolvedBirthYear = birthYear != null
            ? Math.max(0, birthYear.intValue())
            : (existing != null ? Math.max(0, existing.birthYear) : 0);
        int resolvedSex = existing != null ? existing.sex : 0;
        int resolvedReferralId = existing != null ? Math.max(0, existing.referralId) : 0;
        long resolvedFbUser = existing != null ? Math.max(0L, existing.fbUser) : 0L;
        String resolvedAccessToken = existing != null ? safe(existing.accessToken) : "";
        int resolvedWantsEmail = wantsEmail != null
            ? (wantsEmail.booleanValue() ? 1 : 0)
            : ((existing != null && existing.wantsEmail) ? 1 : 0);

        String sql =
            "INSERT INTO account_profiles (" +
            "player_id, first_name, sur_name, email, city, presentation_picture_url, country_id, " +
            "birth_date, birth_month, birth_year, sex, wants_email, referral_id, fb_user, access_token, created_at" +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM account_profiles WHERE player_id = ?), ?)) " +
            "ON CONFLICT(player_id) DO UPDATE SET " +
            "first_name = excluded.first_name, " +
            "sur_name = excluded.sur_name, " +
            "email = excluded.email, " +
            "city = excluded.city, " +
            "presentation_picture_url = excluded.presentation_picture_url, " +
            "country_id = excluded.country_id, " +
            "birth_date = excluded.birth_date, " +
            "birth_month = excluded.birth_month, " +
            "birth_year = excluded.birth_year, " +
            "sex = excluded.sex, " +
            "wants_email = excluded.wants_email, " +
            "referral_id = excluded.referral_id, " +
            "fb_user = excluded.fb_user, " +
            "access_token = excluded.access_token";
        long now = System.currentTimeMillis();
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            statement.setString(2, resolvedFirstName);
            statement.setString(3, resolvedSurName);
            statement.setString(4, resolvedEmail);
            statement.setString(5, resolvedCity);
            statement.setString(6, resolvedPresentationPictureUrl);
            statement.setInt(7, resolvedCountryId);
            statement.setInt(8, resolvedBirthDate);
            statement.setInt(9, resolvedBirthMonth);
            statement.setInt(10, resolvedBirthYear);
            statement.setInt(11, resolvedSex);
            statement.setInt(12, resolvedWantsEmail);
            statement.setInt(13, resolvedReferralId);
            statement.setLong(14, resolvedFbUser);
            statement.setString(15, resolvedAccessToken);
            statement.setInt(16, playerId);
            statement.setLong(17, now);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to store player information", e);
        }
    }

    public synchronized void updateUserPassword(int userId, String newPassword) throws IOException {
        if (userId <= 0) {
            return;
        }
        String normalized = newPassword == null ? "" : newPassword;
        if (normalized.trim().isEmpty()) {
            return;
        }
        updatePasswordHash(userId, hashPassword(normalized));
    }

    public synchronized int getAccountAgeDays(int playerId) {
        AccountProfileRecord profile = loadAccountProfile(playerId);
        if (profile == null || profile.createdAt <= 0L) {
            return 0;
        }
        long elapsedMillis = Math.max(0L, System.currentTimeMillis() - profile.createdAt);
        return (int) (elapsedMillis / 86_400_000L);
    }

    public synchronized int getPlayerAgeYears(int playerId) {
        AccountProfileRecord profile = loadAccountProfile(playerId);
        if (profile == null) {
            return 0;
        }
        int year = profile.birthYear;
        int month = profile.birthMonth;
        int day = profile.birthDate;
        if (year < 1900 || month < 1 || month > 12 || day < 1 || day > 31) {
            return 0;
        }
        try {
            LocalDate birth = LocalDate.of(year, month, day);
            LocalDate today = LocalDate.now();
            if (birth.isAfter(today)) {
                return 0;
            }
            return Math.max(0, Period.between(birth, today).getYears());
        } catch (RuntimeException ignored) {
            return 0;
        }
    }

    public synchronized int getReferralId(int playerId) {
        AccountProfileRecord profile = loadAccountProfile(playerId);
        return profile != null ? profile.referralId : 0;
    }

    public synchronized byte getPlayerSex(int playerId) {
        AccountProfileRecord profile = loadAccountProfile(playerId);
        return profile != null ? profile.sex : 0;
    }

    public synchronized ArrayList<Building> loadBuildings(int villageId) {
        ArrayList<Building> result = new ArrayList<>();
        ArrayList<Short> invalidBuildingIds = new ArrayList<>();
        String sql =
            "SELECT building_id, building_type, remaining_points, tile_x, tile_z, parameters, rotation, consumed, placed_on_map, ready, delay_start, delay_end, ingredients " +
            "FROM buildings WHERE village_id = ? ORDER BY building_id";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, villageId);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    short buildingId = rs.getShort("building_id");
                    short buildingType = rs.getShort("building_type");
                    if (!isSupportedBuildingType(buildingType)) {
                        invalidBuildingIds.add(buildingId);
                        continue;
                    }
                    result.add(new Building(
                        buildingId,
                        buildingType,
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
            purgeInvalidBuildings(connection, villageId, invalidBuildingIds);
        } catch (SQLException ignored) {
            // Keep empty fallback.
        }
        return result;
    }

    public synchronized ArrayList<Building> promoteExpiredBuildingCooldowns(int villageId,
                                                                            long nowMillis,
                                                                            Map<Short, Integer> requiredIngredientCountsByBuildingType,
                                                                            Set<Short> resourceBuildingTypes) throws IOException {
        ArrayList<Building> promoted = new ArrayList<>();
        if (villageId <= 0) {
            return promoted;
        }
        Map<Short, Integer> requiredIngredients =
            requiredIngredientCountsByBuildingType != null ? requiredIngredientCountsByBuildingType : Collections.emptyMap();
        Set<Short> normalizedResourceTypes =
            resourceBuildingTypes != null ? resourceBuildingTypes : Collections.emptySet();
        long cutoff = Math.max(0L, nowMillis);
        String selectSql =
            "SELECT building_id, building_type, remaining_points, tile_x, tile_z, parameters, rotation, consumed, placed_on_map, delay_start, delay_end, ingredients " +
            "FROM buildings WHERE village_id = ? AND ready = 0 AND delay_end > 0 AND delay_end <= ? ORDER BY building_id";
        String updateSql = "UPDATE buildings SET ready = 1, consumed = ? WHERE village_id = ? AND building_id = ? AND ready = 0";

        try (Connection connection = openConnection()) {
            boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try (PreparedStatement selectStatement = connection.prepareStatement(selectSql);
                 PreparedStatement updateStatement = connection.prepareStatement(updateSql)) {
                selectStatement.setInt(1, villageId);
                selectStatement.setLong(2, cutoff);
                try (ResultSet rs = selectStatement.executeQuery()) {
                    while (rs.next()) {
                        short buildingType = (short) rs.getInt("building_type");
                        long ingredients = rs.getLong("ingredients");
                        int requiredIngredientCount = Math.max(0, requiredIngredients.getOrDefault(buildingType, 0));
                        if (!hasAllRequiredIngredientBits(ingredients, requiredIngredientCount)) {
                            continue;
                        }
                        boolean consumed = rs.getInt("consumed") != 0;
                        if (consumed && normalizedResourceTypes.contains(buildingType)) {
                            consumed = false;
                        }
                        promoted.add(new Building(
                            (short) rs.getInt("building_id"),
                            buildingType,
                            rs.getFloat("remaining_points"),
                            (byte) rs.getInt("tile_x"),
                            (byte) rs.getInt("tile_z"),
                            nullableBytes(rs.getBytes("parameters")),
                            (byte) rs.getInt("rotation"),
                            consumed,
                            rs.getInt("placed_on_map") != 0,
                            true,
                            rs.getLong("delay_start"),
                            rs.getLong("delay_end"),
                            ingredients
                        ));
                    }
                }

                for (Building building : promoted) {
                    updateStatement.setInt(1, building.isConsumed() ? 1 : 0);
                    updateStatement.setInt(2, villageId);
                    updateStatement.setShort(3, building.getBuildingID());
                    updateStatement.addBatch();
                }
                if (!promoted.isEmpty()) {
                    updateStatement.executeBatch();
                }

                connection.commit();
                return promoted;
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(previousAutoCommit);
            }
        } catch (SQLException e) {
            throw new IOException("Failed to promote expired building cooldowns", e);
        }
    }

    public synchronized int restoreStuckResourceCooldownRows(Set<Short> resourceBuildingTypes) throws IOException {
        if (resourceBuildingTypes == null || resourceBuildingTypes.isEmpty()) {
            return 0;
        }
        ArrayList<Short> normalizedTypes = new ArrayList<>();
        for (Short buildingType : resourceBuildingTypes) {
            if (buildingType == null) {
                continue;
            }
            normalizedTypes.add(buildingType);
        }
        if (normalizedTypes.isEmpty()) {
            return 0;
        }

        StringBuilder sql = new StringBuilder(
            "UPDATE buildings SET ready = 1, delay_start = 0, delay_end = 0, consumed = 0 " +
            "WHERE ready = 0 AND delay_end = 0 AND building_type IN ("
        );
        for (int i = 0; i < normalizedTypes.size(); i++) {
            if (i > 0) {
                sql.append(',');
            }
            sql.append('?');
        }
        sql.append(')');

        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            int index = 1;
            for (Short buildingType : normalizedTypes) {
                statement.setShort(index++, buildingType);
            }
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to restore stuck resource cooldown rows", e);
        }
    }

    public synchronized ArrayList<Building> restoreStuckResourceCooldownRowsInVillage(int villageId,
                                                                                      Set<Short> resourceBuildingTypes) throws IOException {
        ArrayList<Building> restored = new ArrayList<>();
        if (villageId <= 0 || resourceBuildingTypes == null || resourceBuildingTypes.isEmpty()) {
            return restored;
        }
        ArrayList<Short> normalizedTypes = new ArrayList<>();
        for (Short buildingType : resourceBuildingTypes) {
            if (buildingType == null) {
                continue;
            }
            normalizedTypes.add(buildingType);
        }
        if (normalizedTypes.isEmpty()) {
            return restored;
        }

        StringBuilder selectSql = new StringBuilder(
            "SELECT building_id, building_type, remaining_points, tile_x, tile_z, parameters, rotation, consumed, placed_on_map, ingredients " +
            "FROM buildings WHERE village_id = ? AND ready = 0 AND delay_end = 0 AND building_type IN ("
        );
        StringBuilder updateSql = new StringBuilder(
            "UPDATE buildings SET ready = 1, delay_start = 0, delay_end = 0, consumed = 0 " +
            "WHERE village_id = ? AND building_id = ? AND ready = 0 AND delay_end = 0"
        );
        for (int i = 0; i < normalizedTypes.size(); i++) {
            if (i > 0) {
                selectSql.append(',');
            }
            selectSql.append('?');
        }
        selectSql.append(") ORDER BY building_id");

        try (Connection connection = openConnection()) {
            boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try (PreparedStatement selectStatement = connection.prepareStatement(selectSql.toString());
                 PreparedStatement updateStatement = connection.prepareStatement(updateSql.toString())) {
                int index = 1;
                selectStatement.setInt(index++, villageId);
                for (Short buildingType : normalizedTypes) {
                    selectStatement.setShort(index++, buildingType);
                }
                try (ResultSet rs = selectStatement.executeQuery()) {
                    while (rs.next()) {
                        restored.add(new Building(
                            (short) rs.getInt("building_id"),
                            (short) rs.getInt("building_type"),
                            rs.getFloat("remaining_points"),
                            (byte) rs.getInt("tile_x"),
                            (byte) rs.getInt("tile_z"),
                            nullableBytes(rs.getBytes("parameters")),
                            (byte) rs.getInt("rotation"),
                            false,
                            rs.getInt("placed_on_map") != 0,
                            true,
                            0L,
                            0L,
                            rs.getLong("ingredients")
                        ));
                    }
                }

                for (Building building : restored) {
                    updateStatement.setInt(1, villageId);
                    updateStatement.setShort(2, building.getBuildingID());
                    updateStatement.addBatch();
                }
                if (!restored.isEmpty()) {
                    updateStatement.executeBatch();
                }
                connection.commit();
                return restored;
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(previousAutoCommit);
            }
        } catch (SQLException e) {
            throw new IOException("Failed to restore stuck resource cooldown rows in village", e);
        }
    }

    public synchronized Short loadBuildingType(int villageId, short buildingId) throws IOException {
        String sql = "SELECT building_type FROM buildings WHERE village_id = ? AND building_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, villageId);
            statement.setShort(2, buildingId);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return rs.getShort("building_type");
                }
                return null;
            }
        } catch (SQLException e) {
            throw new IOException("Failed to load building type", e);
        }
    }

    public synchronized ArrayList<ResourcePlacement> loadResourcePlacementsForType(short buildingType) {
        ArrayList<ResourcePlacement> placements = new ArrayList<>();
        if (buildingType <= 0) {
            return placements;
        }
        String templateVillageSql =
            "SELECT village_id FROM buildings " +
            "WHERE building_type = ? AND placed_on_map = 1 " +
            "GROUP BY village_id " +
            "ORDER BY COUNT(*) DESC, village_id ASC LIMIT 1";
        String placementsSql =
            "SELECT tile_x, tile_z, rotation FROM buildings " +
            "WHERE village_id = ? AND building_type = ? AND placed_on_map = 1 " +
            "ORDER BY building_id";
        try (Connection connection = openConnection();
             PreparedStatement templateVillageStatement = connection.prepareStatement(templateVillageSql)) {
            templateVillageStatement.setShort(1, buildingType);
            int templateVillageId = 0;
            try (ResultSet rs = templateVillageStatement.executeQuery()) {
                if (rs.next()) {
                    templateVillageId = rs.getInt("village_id");
                }
            }
            if (templateVillageId <= 0) {
                return placements;
            }
            try (PreparedStatement placementsStatement = connection.prepareStatement(placementsSql)) {
                placementsStatement.setInt(1, templateVillageId);
                placementsStatement.setShort(2, buildingType);
                try (ResultSet rs = placementsStatement.executeQuery()) {
                    while (rs.next()) {
                        placements.add(new ResourcePlacement(
                            (byte) rs.getInt("tile_x"),
                            (byte) rs.getInt("tile_z"),
                            (byte) rs.getInt("rotation")
                        ));
                    }
                }
            }
        } catch (SQLException ignored) {
            // Keep empty fallback.
        }
        return placements;
    }

    public synchronized ArrayList<Building> seedVillageWildResourcesIfMissing(int villageId,
                                                                               short resourceBuildingType,
                                                                               ArrayList<ResourcePlacement> placements,
                                                                               Set<Short> allResourceBuildingTypes) throws IOException {
        ArrayList<Building> seeded = new ArrayList<>();
        if (villageId <= 0 || resourceBuildingType <= 0 || placements == null || placements.isEmpty()) {
            return seeded;
        }
        ArrayList<Short> normalizedTypes = new ArrayList<>();
        if (allResourceBuildingTypes != null) {
            for (Short type : allResourceBuildingTypes) {
                if (type == null || type <= 0) {
                    continue;
                }
                normalizedTypes.add(type);
            }
        }
        if (normalizedTypes.isEmpty()) {
            normalizedTypes.add(resourceBuildingType);
        }
        StringBuilder existsSql = new StringBuilder(
            "SELECT 1 FROM buildings WHERE village_id = ? AND building_type IN ("
        );
        for (int i = 0; i < normalizedTypes.size(); i++) {
            if (i > 0) {
                existsSql.append(',');
            }
            existsSql.append('?');
        }
        existsSql.append(") LIMIT 1");
        String usedIdsSql = "SELECT building_id FROM buildings WHERE village_id = ?";
        String insertSql =
            "INSERT INTO buildings (village_id, building_id, building_type, remaining_points, tile_x, tile_z, parameters, rotation, consumed, placed_on_map, ready, delay_start, delay_end, ingredients) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection connection = openConnection()) {
            boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try (PreparedStatement existsStatement = connection.prepareStatement(existsSql.toString())) {
                int existsIndex = 1;
                existsStatement.setInt(existsIndex++, villageId);
                for (Short type : normalizedTypes) {
                    existsStatement.setShort(existsIndex++, type);
                }
                try (ResultSet rs = existsStatement.executeQuery()) {
                    if (rs.next()) {
                        connection.commit();
                        return seeded;
                    }
                }
            }

            HashSet<Short> usedIds = new HashSet<>();
            try (PreparedStatement usedIdsStatement = connection.prepareStatement(usedIdsSql)) {
                usedIdsStatement.setInt(1, villageId);
                try (ResultSet rs = usedIdsStatement.executeQuery()) {
                    while (rs.next()) {
                        int rawId = rs.getInt("building_id");
                        if (rawId < Short.MIN_VALUE || rawId > Short.MAX_VALUE) {
                            continue;
                        }
                        usedIds.add((short) rawId);
                    }
                }
            }

            short candidate = Short.MIN_VALUE;
            try (PreparedStatement insertStatement = connection.prepareStatement(insertSql)) {
                for (ResourcePlacement placement : placements) {
                    while (usedIds.contains(candidate) && candidate < Short.MAX_VALUE) {
                        candidate++;
                    }
                    if (usedIds.contains(candidate)) {
                        break;
                    }
                    short buildingId = candidate;
                    usedIds.add(buildingId);
                    insertStatement.setInt(1, villageId);
                    insertStatement.setShort(2, buildingId);
                    insertStatement.setShort(3, resourceBuildingType);
                    insertStatement.setFloat(4, 0f);
                    insertStatement.setInt(5, placement.tileX);
                    insertStatement.setInt(6, placement.tileZ);
                    insertStatement.setBytes(7, null);
                    insertStatement.setInt(8, placement.rotation);
                    insertStatement.setInt(9, 0);
                    insertStatement.setInt(10, 1);
                    insertStatement.setInt(11, 1);
                    insertStatement.setLong(12, 0L);
                    insertStatement.setLong(13, 0L);
                    insertStatement.setLong(14, 0L);
                    insertStatement.addBatch();
                    seeded.add(new Building(
                        buildingId,
                        resourceBuildingType,
                        0f,
                        placement.tileX,
                        placement.tileZ,
                        null,
                        placement.rotation,
                        false,
                        true,
                        true,
                        0L,
                        0L,
                        0L
                    ));
                    if (candidate < Short.MAX_VALUE) {
                        candidate++;
                    }
                }
                if (!seeded.isEmpty()) {
                    insertStatement.executeBatch();
                }
            }
            connection.commit();
            return seeded;
        } catch (SQLException e) {
            throw new IOException("Failed to seed wild resources for village", e);
        }
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

    public synchronized AddBuildingPurchaseResult addBuildingWithCreditSpend(int userId,
                                                                             int villageId,
                                                                             Building building,
                                                                             int totalCost,
                                                                             boolean allowCrossVillagePlacement) throws IOException {
        if (building == null || userId <= 0 || villageId <= 0) {
            int amount = userId > 0 ? getSLXCredits(userId) : 0;
            return new AddBuildingPurchaseResult(false, false, amount);
        }
        int safeCost = Math.max(0, totalCost);
        String userSql = "SELECT village_id, slx_credits FROM users WHERE user_id = ?";
        String upsertSql =
            "INSERT INTO buildings (village_id, building_id, building_type, remaining_points, tile_x, tile_z, parameters, rotation, consumed, placed_on_map, ready, delay_start, delay_end, ingredients) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT(village_id, building_id) DO UPDATE SET " +
            "building_type=excluded.building_type, remaining_points=excluded.remaining_points, tile_x=excluded.tile_x, tile_z=excluded.tile_z, " +
            "parameters=excluded.parameters, rotation=excluded.rotation, consumed=excluded.consumed, placed_on_map=excluded.placed_on_map, " +
            "ready=excluded.ready, delay_start=excluded.delay_start, delay_end=excluded.delay_end, ingredients=excluded.ingredients";
        String updateCreditsSql = "UPDATE users SET slx_credits = ? WHERE user_id = ?";

        try (Connection connection = openConnection()) {
            boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                int ownerVillageId = 0;
                int currentCredits = 0;
                boolean userFound = false;
                try (PreparedStatement userStatement = connection.prepareStatement(userSql)) {
                    userStatement.setInt(1, userId);
                    try (ResultSet rs = userStatement.executeQuery()) {
                        if (rs.next()) {
                            ownerVillageId = rs.getInt("village_id");
                            currentCredits = rs.getInt("slx_credits");
                            userFound = true;
                        }
                    }
                }
                if (!userFound) {
                    connection.rollback();
                    return new AddBuildingPurchaseResult(false, false, 0);
                }
                if (!allowCrossVillagePlacement && ownerVillageId != villageId) {
                    connection.rollback();
                    return new AddBuildingPurchaseResult(false, false, currentCredits);
                }
                if (currentCredits < safeCost) {
                    connection.rollback();
                    return new AddBuildingPurchaseResult(false, true, currentCredits);
                }

                try (PreparedStatement upsertStatement = connection.prepareStatement(upsertSql)) {
                    bindUpsertBuildingStatement(upsertStatement, villageId, building);
                    upsertStatement.executeUpdate();
                }

                int creditsAfter = currentCredits - safeCost;
                try (PreparedStatement updateCreditsStatement = connection.prepareStatement(updateCreditsSql)) {
                    updateCreditsStatement.setInt(1, creditsAfter);
                    updateCreditsStatement.setInt(2, userId);
                    updateCreditsStatement.executeUpdate();
                }

                connection.commit();
                return new AddBuildingPurchaseResult(true, false, creditsAfter);
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(previousAutoCommit);
            }
        } catch (SQLException e) {
            throw new IOException("Failed to add building with credit spend", e);
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
                System.out.println("[Server] Ignored UpdateBuilding for missing row villageId=" + villageId +
                    " buildingId=" + buildingId);
            }
        } catch (SQLException e) {
            throw new IOException("Failed to update building", e);
        }
    }

    private boolean isSupportedBuildingType(short buildingType) {
        if (buildingType == 0) {
            return false;
        }
        Map<Short, Integer> knownTypes = buildingBasePricesByType;
        return knownTypes == null || knownTypes.isEmpty() || knownTypes.containsKey(buildingType);
    }

    private void purgeInvalidBuildings(Connection connection, int villageId, ArrayList<Short> buildingIds) {
        if (connection == null || buildingIds == null || buildingIds.isEmpty()) {
            return;
        }
        String sql = "DELETE FROM buildings WHERE village_id = ? AND building_id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (short buildingId : buildingIds) {
                statement.setInt(1, villageId);
                statement.setShort(2, buildingId);
                statement.addBatch();
            }
            int[] results = statement.executeBatch();
            int removed = 0;
            for (int count : results) {
                if (count > 0 || count == Statement.SUCCESS_NO_INFO) {
                    removed++;
                }
            }
            if (removed > 0) {
                System.out.println("[Server] Purged " + removed + " invalid building row(s) from villageId=" + villageId);
            }
        } catch (SQLException e) {
            System.out.println("[Server] Failed to purge invalid building rows for villageId=" + villageId +
                ": " + e.getMessage());
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

    public synchronized RemoveBuildingRefundResult removeBuildingWithCreditRefund(int userId,
                                                                                  int villageId,
                                                                                  short buildingId,
                                                                                  int requestedRefund) throws IOException {
        int safeRequestedRefund = Math.max(0, requestedRefund);
        String userSql = "SELECT village_id, slx_credits FROM users WHERE user_id = ?";
        String buildingSql = "SELECT building_type FROM buildings WHERE village_id = ? AND building_id = ?";
        String deleteSql = "DELETE FROM buildings WHERE village_id = ? AND building_id = ?";
        String updateCreditsSql = "UPDATE users SET slx_credits = ? WHERE user_id = ?";

        try (Connection connection = openConnection()) {
            boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                int ownerVillageId = 0;
                int currentCredits = 0;
                boolean userFound = false;
                try (PreparedStatement userStatement = connection.prepareStatement(userSql)) {
                    userStatement.setInt(1, userId);
                    try (ResultSet rs = userStatement.executeQuery()) {
                        if (rs.next()) {
                            ownerVillageId = rs.getInt("village_id");
                            currentCredits = rs.getInt("slx_credits");
                            userFound = true;
                        }
                    }
                }
                if (!userFound) {
                    connection.rollback();
                    return new RemoveBuildingRefundResult(false, 0, 0, (short) 0);
                }
                if (ownerVillageId != villageId) {
                    connection.rollback();
                    return new RemoveBuildingRefundResult(false, currentCredits, 0, (short) 0);
                }

                short buildingType = 0;
                boolean buildingFound = false;
                try (PreparedStatement buildingStatement = connection.prepareStatement(buildingSql)) {
                    buildingStatement.setInt(1, villageId);
                    buildingStatement.setShort(2, buildingId);
                    try (ResultSet rs = buildingStatement.executeQuery()) {
                        if (rs.next()) {
                            buildingType = rs.getShort("building_type");
                            buildingFound = true;
                        }
                    }
                }
                if (!buildingFound) {
                    connection.rollback();
                    return new RemoveBuildingRefundResult(false, currentCredits, 0, (short) 0);
                }

                int deleted;
                try (PreparedStatement deleteStatement = connection.prepareStatement(deleteSql)) {
                    deleteStatement.setInt(1, villageId);
                    deleteStatement.setShort(2, buildingId);
                    deleted = deleteStatement.executeUpdate();
                }
                if (deleted == 0) {
                    connection.rollback();
                    return new RemoveBuildingRefundResult(false, currentCredits, 0, buildingType);
                }

                int basePrice = Math.max(0, buildingBasePricesByType.getOrDefault(buildingType, 0));
                int maxServerRefund = (basePrice * 33) / 100;
                int refundGranted = Math.min(safeRequestedRefund, maxServerRefund);
                int creditsAfter = clampToInt((long) currentCredits + refundGranted);

                try (PreparedStatement updateCreditsStatement = connection.prepareStatement(updateCreditsSql)) {
                    updateCreditsStatement.setInt(1, creditsAfter);
                    updateCreditsStatement.setInt(2, userId);
                    updateCreditsStatement.executeUpdate();
                }

                connection.commit();
                return new RemoveBuildingRefundResult(true, creditsAfter, refundGranted, buildingType);
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(previousAutoCommit);
            }
        } catch (SQLException e) {
            throw new IOException("Failed to remove building with refund", e);
        }
    }

    public synchronized void setBuildingBasePrices(Map<Short, Integer> pricesByType) {
        if (pricesByType == null || pricesByType.isEmpty()) {
            buildingBasePricesByType = Collections.emptyMap();
            return;
        }
        Map<Short, Integer> sanitized = new HashMap<>();
        for (Map.Entry<Short, Integer> entry : pricesByType.entrySet()) {
            if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            sanitized.put(entry.getKey(), Math.max(0, entry.getValue()));
        }
        buildingBasePricesByType = Collections.unmodifiableMap(sanitized);
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

    public synchronized ArrayList<SimulatedCharacterData> loadCharacters(int villageId) {
        ArrayList<SimulatedCharacterData> result = new ArrayList<>();
        String sql =
            "SELECT character_id, pos_x, pos_z, family_id, home_id, data, in_building_id, type, first_name, sur_name, consumed " +
            "FROM simulated_characters WHERE village_id = ? ORDER BY character_id";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, villageId);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    result.add(new SimulatedCharacterData(
                        (short) rs.getInt("character_id"),
                        (short) rs.getInt("pos_x"),
                        (short) rs.getInt("pos_z"),
                        nullableBytes(rs.getBytes("data")),
                        (short) rs.getInt("family_id"),
                        (short) rs.getInt("home_id"),
                        (short) rs.getInt("in_building_id"),
                        safe(rs.getString("first_name")),
                        safe(rs.getString("sur_name")),
                        (short) rs.getInt("type"),
                        rs.getInt("consumed") != 0
                    ));
                }
            }
        } catch (SQLException ignored) {
            // Keep empty fallback.
        }
        return result;
    }

    public synchronized void upsertCharacter(int villageId, SimulatedCharacterData character) throws IOException {
        if (character == null) {
            return;
        }
        String sql =
            "INSERT INTO simulated_characters (village_id, character_id, pos_x, pos_z, family_id, home_id, data, in_building_id, type, first_name, sur_name, consumed) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT(village_id, character_id) DO UPDATE SET " +
            "pos_x=excluded.pos_x, pos_z=excluded.pos_z, family_id=excluded.family_id, home_id=excluded.home_id, data=excluded.data, " +
            "in_building_id=excluded.in_building_id, type=excluded.type, first_name=excluded.first_name, sur_name=excluded.sur_name, consumed=excluded.consumed";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, villageId);
            statement.setShort(2, character.getCharacterID());
            statement.setShort(3, character.getPosX());
            statement.setShort(4, character.getPosZ());
            statement.setShort(5, character.getFamilyID());
            statement.setShort(6, character.getHomeID());
            statement.setBytes(7, nullableBytes(character.getData()));
            statement.setShort(8, character.getInBuildingID());
            statement.setShort(9, character.getType());
            statement.setString(10, safe(character.getFirstName()));
            statement.setString(11, safe(character.getSurName()));
            statement.setInt(12, character.isConsumed() ? 1 : 0);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to upsert character", e);
        }
    }

    public synchronized void upsertCharacters(int villageId, ArrayList characters) throws IOException {
        if (characters == null || characters.isEmpty()) {
            return;
        }
        for (Object object : characters) {
            if (!(object instanceof SimulatedCharacterData)) {
                continue;
            }
            upsertCharacter(villageId, (SimulatedCharacterData) object);
        }
    }

    public synchronized void removeCharacter(int villageId, short characterId) throws IOException {
        String sql = "DELETE FROM simulated_characters WHERE village_id = ? AND character_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, villageId);
            statement.setShort(2, characterId);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to remove character", e);
        }
    }

    public synchronized HeightMapRecord loadHeightMap(int villageId, byte[] fallbackHeightData) {
        String sql = "SELECT height_data, type, size FROM village_heightmaps WHERE village_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, villageId);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    byte[] stored = nullableBytes(rs.getBytes("height_data"));
                    byte type = (byte) normalizeVillageThemeValue(rs.getInt("type"));
                    byte size = (byte) normalizeVillageBoundsSizeValue(rs.getInt("size"));
                    if (isUsableHeightData(stored, fallbackHeightData)) {
                        return new HeightMapRecord(
                            stored,
                            type,
                            size
                        );
                    }
                    // Keep stored type/size metadata (e.g. expanded village bounds) even if bytes are empty/corrupt.
                    return new HeightMapRecord(fallbackHeightData, type, size);
                }
            }
        } catch (SQLException ignored) {
            // Fall back to default.
        }
        return new HeightMapRecord(
            fallbackHeightData,
            (byte) DEFAULT_VILLAGE_THEME,
            (byte) DEFAULT_VILLAGE_BOUNDS_SIZE
        );
    }

    public synchronized byte loadVillageTheme(int villageId) {
        String sql = "SELECT type FROM village_heightmaps WHERE village_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, villageId);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return (byte) normalizeVillageThemeValue(rs.getInt("type"));
                }
            }
        } catch (SQLException ignored) {
            // Keep default fallback.
        }
        return (byte) DEFAULT_VILLAGE_THEME;
    }

    public synchronized void storeHeightMap(int villageId, byte[] heightData, byte type, byte size) throws IOException {
        String selectSql = "SELECT type, size FROM village_heightmaps WHERE village_id = ?";
        String upsertSql =
            "INSERT INTO village_heightmaps (village_id, height_data, type, size) VALUES (?, ?, ?, ?) " +
            "ON CONFLICT(village_id) DO UPDATE SET " +
            "height_data=CASE WHEN length(excluded.height_data) > 0 THEN excluded.height_data ELSE village_heightmaps.height_data END, " +
            "type=excluded.type, size=excluded.size";
        try (Connection connection = openConnection()) {
            int existingType = DEFAULT_VILLAGE_THEME;
            int existingSize = DEFAULT_VILLAGE_BOUNDS_SIZE;
            try (PreparedStatement selectStatement = connection.prepareStatement(selectSql)) {
                selectStatement.setInt(1, villageId);
                try (ResultSet rs = selectStatement.executeQuery()) {
                    if (rs.next()) {
                        existingType = normalizeVillageThemeValue(rs.getInt("type"));
                        existingSize = normalizeVillageBoundsSizeValue(rs.getInt("size"));
                    }
                }
            }

            int normalizedType = type >= 0 ? normalizeVillageThemeValue(type) : existingType;
            int normalizedSize = size > 0 ? size : existingSize;
            normalizedSize = normalizeVillageBoundsSizeValue(normalizedSize);

            try (PreparedStatement statement = connection.prepareStatement(upsertSql)) {
                statement.setInt(1, villageId);
                statement.setBytes(2, nullableBytes(heightData));
                statement.setInt(3, normalizedType);
                statement.setInt(4, normalizedSize);
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IOException("Failed to store height map", e);
        }
    }

    private static int normalizeVillageThemeValue(int theme) {
        return Math.max(DEFAULT_VILLAGE_THEME, theme);
    }

    private static int normalizeVillageBoundsSizeValue(int size) {
        return size > 0 ? size : DEFAULT_VILLAGE_BOUNDS_SIZE;
    }

    private static boolean isUsableHeightData(byte[] data, byte[] fallbackHeightData) {
        byte[] candidate = nullableBytes(data);
        if (candidate.length == 0) {
            return false;
        }
        if (fallbackHeightData != null && fallbackHeightData.length > 0 && candidate.length != fallbackHeightData.length) {
            return false;
        }
        for (byte value : candidate) {
            if (value != 0) {
                return true;
            }
        }
        return false;
    }

    public synchronized AvatarDetails loadAvatarDetails(int playerId, String fallbackName, int fallbackLevel) {
        UserRecord user = findByUserId(playerId);
        if (user == null) {
            return new AvatarDetails(playerId, fallbackName, (short) 0, (short) 0, defaultAvatarBlob(), "", "", fallbackLevel);
        }

        String sql = "SELECT avatar_data, mount_type, mount_name, pet_type, pet_name FROM avatar_state WHERE player_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    byte[] avatarData = nullableBytes(rs.getBytes("avatar_data"));
                    if (avatarData.length == 0) {
                        avatarData = defaultAvatarBlob();
                    }
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
        return new AvatarDetails(user.userId, user.username, (short) 0, (short) 0, defaultAvatarBlob(), "", "", user.level);
    }

    public synchronized void storeAvatarData(int playerId, byte[] avatarData) throws IOException {
        // Preserve null so upsert can treat it as "no change" via COALESCE.
        upsertAvatarState(playerId, avatarData, null, null, null, null);
    }

    public synchronized void storePlayerClothes(int playerId, Clothes clothes) throws IOException {
        if (clothes == null) {
            return;
        }
        String sql =
            "INSERT INTO player_clothes (player_id, cloth_id, hue, saturation, brightness, opacity) " +
            "VALUES (?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT(player_id, cloth_id) DO UPDATE SET " +
            "hue = excluded.hue, " +
            "saturation = excluded.saturation, " +
            "brightness = excluded.brightness, " +
            "opacity = excluded.opacity";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            statement.setLong(2, clothes.getClothID());
            statement.setFloat(3, clothes.getHue());
            statement.setFloat(4, clothes.getSaturation());
            statement.setFloat(5, clothes.getBrightness());
            statement.setFloat(6, clothes.getOpacity());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to store player clothes", e);
        }
    }

    public synchronized ArrayList<Clothes> loadPlayerClothes(int playerId) {
        ArrayList<Clothes> clothes = new ArrayList<>();
        String sql =
            "SELECT cloth_id, hue, saturation, brightness, opacity " +
            "FROM player_clothes WHERE player_id = ? ORDER BY cloth_id";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    clothes.add(new Clothes(
                        0,
                        playerId,
                        rs.getLong("cloth_id"),
                        rs.getFloat("hue"),
                        rs.getFloat("saturation"),
                        rs.getFloat("brightness"),
                        rs.getFloat("opacity")
                    ));
                }
            }
        } catch (SQLException ignored) {
            // Return what we could load.
        }
        if (!clothes.isEmpty()) {
            return clothes;
        }

        // Legacy client flow may only store avatarData; derive clothes list from embedded model/texture layers.
        String avatarSql = "SELECT avatar_data FROM avatar_state WHERE player_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(avatarSql)) {
            statement.setInt(1, playerId);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    byte[] avatarData = nullableBytes(rs.getBytes("avatar_data"));
                    return extractClothesFromAvatarBlob(playerId, avatarData);
                }
            }
        } catch (SQLException ignored) {
            // Keep empty fallback.
        }
        return clothes;
    }

    public synchronized void storeMount(int playerId, short mountType, String mountName) throws IOException {
        upsertAvatarState(playerId, null, mountType, safe(mountName), null, null);
    }

    public synchronized void storePet(int playerId, short petType, String petName) throws IOException {
        upsertAvatarState(playerId, null, null, null, petType, safe(petName));
    }

    public synchronized void grantCompanionOwnership(int playerId,
                                                     String companionKind,
                                                     short companionType,
                                                     String source) throws IOException {
        String normalizedKind = normalizeCompanionKind(companionKind);
        if (playerId <= 0 || normalizedKind.isEmpty() || companionType <= 0) {
            return;
        }
        String sql =
            "INSERT INTO companion_ownership (player_id, companion_kind, companion_type, acquired_at, source) " +
            "VALUES (?, ?, ?, ?, ?) " +
            "ON CONFLICT(player_id, companion_kind, companion_type) DO UPDATE SET " +
            "acquired_at = CASE " +
            "  WHEN companion_ownership.acquired_at <= 0 THEN excluded.acquired_at " +
            "  ELSE MIN(companion_ownership.acquired_at, excluded.acquired_at) " +
            "END, " +
            "source = CASE " +
            "  WHEN excluded.source = '' THEN companion_ownership.source " +
            "  WHEN companion_ownership.source = '' THEN excluded.source " +
            "  ELSE companion_ownership.source " +
            "END";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            statement.setString(2, normalizedKind);
            statement.setInt(3, companionType);
            statement.setLong(4, System.currentTimeMillis());
            statement.setString(5, safe(source));
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to grant companion ownership", e);
        }
    }

    public synchronized boolean isCompanionOwned(int playerId, String companionKind, short companionType) {
        String normalizedKind = normalizeCompanionKind(companionKind);
        if (playerId <= 0 || normalizedKind.isEmpty() || companionType <= 0) {
            return companionType <= 0;
        }
        String sql =
            "SELECT 1 FROM companion_ownership " +
            "WHERE player_id = ? AND companion_kind = ? AND companion_type = ? " +
            "LIMIT 1";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            statement.setString(2, normalizedKind);
            statement.setInt(3, companionType);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException ignored) {
            return false;
        }
    }

    public synchronized Map<String, ArrayList<Integer>> loadCompanionOwnership(int playerId) {
        HashMap<String, ArrayList<Integer>> result = new HashMap<>();
        result.put("mount", new ArrayList<>());
        result.put("pet", new ArrayList<>());
        if (playerId <= 0) {
            return result;
        }
        String sql =
            "SELECT companion_kind, companion_type " +
            "FROM companion_ownership " +
            "WHERE player_id = ? " +
            "ORDER BY companion_kind ASC, companion_type ASC";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    String normalizedKind = normalizeCompanionKind(rs.getString("companion_kind"));
                    int companionType = rs.getInt("companion_type");
                    if (normalizedKind.isEmpty() || companionType <= 0) {
                        continue;
                    }
                    ArrayList<Integer> list = result.computeIfAbsent(normalizedKind, ignored -> new ArrayList<>());
                    if (!list.contains(companionType)) {
                        list.add(companionType);
                    }
                }
            }
        } catch (SQLException ignored) {
            return result;
        }
        return result;
    }

    public synchronized boolean updateVillageName(int playerId, String villageName) throws IOException {
        String normalized = safe(villageName).trim();
        if (playerId <= 0 || normalized.isEmpty()) {
            return false;
        }
        if (normalized.length() > 64) {
            normalized = normalized.substring(0, 64);
        }
        String sql = "UPDATE users SET village_name = ? WHERE user_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, normalized);
            statement.setInt(2, playerId);
            return statement.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IOException("Failed to update village name", e);
        }
    }

    public synchronized long storeMidi(int playerId, boolean isGift, byte[] data) throws IOException {
        if (playerId <= 0) {
            return 0L;
        }
        String sql =
            "INSERT INTO midi_songs (owner_player_id, is_gift, data, created_at) VALUES (?, ?, ?, ?)";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            statement.setInt(1, playerId);
            statement.setInt(2, isGift ? 1 : 0);
            statement.setBytes(3, nullableBytes(data));
            statement.setLong(4, System.currentTimeMillis());
            statement.executeUpdate();
            try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    return generatedKeys.getLong(1);
                }
            }
            return 0L;
        } catch (SQLException e) {
            throw new IOException("Failed to store midi", e);
        }
    }

    public synchronized MidiRecord loadMidi(long songId) {
        if (songId <= 0L) {
            return null;
        }
        String sql = "SELECT song_id, owner_player_id, is_gift, data FROM midi_songs WHERE song_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, songId);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new MidiRecord(
                    rs.getLong("song_id"),
                    rs.getInt("owner_player_id"),
                    rs.getInt("is_gift") != 0,
                    nullableBytes(rs.getBytes("data"))
                );
            }
        } catch (SQLException ignored) {
            return null;
        }
    }

    public synchronized long storeQuest(int playerId, int targetPlayerId, short type, byte[] data, boolean finished) throws IOException {
        if (playerId <= 0) {
            return 0L;
        }
        long now = System.currentTimeMillis();
        String sql =
            "INSERT INTO quests (player_id, target_player_id, quest_type, data, finished, created_at, updated_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            statement.setInt(1, playerId);
            statement.setInt(2, Math.max(0, targetPlayerId));
            statement.setInt(3, type);
            statement.setBytes(4, nullableBytes(data));
            statement.setInt(5, finished ? 1 : 0);
            statement.setLong(6, now);
            statement.setLong(7, now);
            statement.executeUpdate();
            try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    return generatedKeys.getLong(1);
                }
            }
            return 0L;
        } catch (SQLException e) {
            throw new IOException("Failed to store quest", e);
        }
    }

    public synchronized int finishQuestsByType(int playerId, short type) throws IOException {
        if (playerId <= 0) {
            return 0;
        }
        String sql =
            "UPDATE quests SET finished = 1, updated_at = ? " +
            "WHERE player_id = ? AND quest_type = ? AND finished = 0";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, System.currentTimeMillis());
            statement.setInt(2, playerId);
            statement.setInt(3, type);
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to finish quests", e);
        }
    }

    public synchronized boolean removeQuest(int playerId, long questId) throws IOException {
        if (playerId <= 0 || questId <= 0L) {
            return false;
        }
        String sql = "DELETE FROM quests WHERE player_id = ? AND quest_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            statement.setLong(2, questId);
            return statement.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IOException("Failed to remove quest", e);
        }
    }

    public synchronized ArrayList<QuestRecord> loadQuests(int playerId) {
        ArrayList<QuestRecord> quests = new ArrayList<>();
        if (playerId <= 0) {
            return quests;
        }
        String sql =
            "SELECT quest_id, player_id, target_player_id, quest_type, data, finished " +
            "FROM quests WHERE player_id = ? ORDER BY quest_id DESC";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    quests.add(
                        new QuestRecord(
                            rs.getLong("quest_id"),
                            rs.getInt("player_id"),
                            rs.getInt("target_player_id"),
                            (short) rs.getInt("quest_type"),
                            nullableBytes(rs.getBytes("data")),
                            rs.getInt("finished") != 0
                        )
                    );
                }
            }
        } catch (SQLException ignored) {
            // Keep empty fallback.
        }
        return quests;
    }

    public synchronized void storePointWalkAnswer(int villageId, int playerId, short buildingId, byte answer, boolean wasCorrectAnswer) throws IOException {
        if (villageId <= 0 || playerId <= 0) {
            return;
        }
        String sql =
            "INSERT INTO point_walk_answers (village_id, player_id, building_id, answer, was_correct, created_at, updated_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT(village_id, player_id, building_id) DO UPDATE SET " +
            "answer = excluded.answer, " +
            "was_correct = excluded.was_correct, " +
            "updated_at = excluded.updated_at";
        long now = System.currentTimeMillis();
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, villageId);
            statement.setInt(2, playerId);
            statement.setInt(3, buildingId);
            statement.setInt(4, answer);
            statement.setInt(5, wasCorrectAnswer ? 1 : 0);
            statement.setLong(6, now);
            statement.setLong(7, now);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to store pointwalk answer", e);
        }
    }

    public synchronized int clearPointWalkAnswersInVillage(int villageId) throws IOException {
        if (villageId <= 0) {
            return 0;
        }
        String sql = "DELETE FROM point_walk_answers WHERE village_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, villageId);
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to clear pointwalk answers", e);
        }
    }

    public synchronized PointWalkQuestionStatsRecord loadPointWalkQuestionStatistics(int villageId, short buildingId) {
        if (villageId <= 0) {
            return new PointWalkQuestionStatsRecord(0, 0, 0, 0, 0);
        }
        String sql =
            "SELECT " +
            "SUM(CASE WHEN was_correct <> 0 THEN 1 ELSE 0 END) AS nr_correct, " +
            "SUM(CASE WHEN was_correct = 0 THEN 1 ELSE 0 END) AS nr_wrong, " +
            "SUM(CASE WHEN answer IN (1, 49) THEN 1 ELSE 0 END) AS nr_1, " +
            "SUM(CASE WHEN answer IN (0, 88, 120) THEN 1 ELSE 0 END) AS nr_x, " +
            "SUM(CASE WHEN answer IN (2, 50) THEN 1 ELSE 0 END) AS nr_2 " +
            "FROM point_walk_answers WHERE village_id = ? AND building_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, villageId);
            statement.setInt(2, buildingId);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    return new PointWalkQuestionStatsRecord(0, 0, 0, 0, 0);
                }
                return new PointWalkQuestionStatsRecord(
                    rs.getInt("nr_correct"),
                    rs.getInt("nr_wrong"),
                    rs.getInt("nr_1"),
                    rs.getInt("nr_x"),
                    rs.getInt("nr_2")
                );
            }
        } catch (SQLException ignored) {
            return new PointWalkQuestionStatsRecord(0, 0, 0, 0, 0);
        }
    }

    public synchronized ArrayList<Short> loadPointWalkQuestionIdsForPlayer(int villageId, int playerId) {
        ArrayList<Short> buildingIds = new ArrayList<>();
        if (villageId <= 0 || playerId <= 0) {
            return buildingIds;
        }
        String sql =
            "SELECT building_id FROM point_walk_answers " +
            "WHERE village_id = ? AND player_id = ? ORDER BY building_id";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, villageId);
            statement.setInt(2, playerId);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    buildingIds.add((short) rs.getInt("building_id"));
                }
            }
        } catch (SQLException ignored) {
            // Keep empty fallback.
        }
        return buildingIds;
    }

    public synchronized ArrayList<Byte> loadPointWalkAnswersForPlayer(int villageId, int playerId) {
        ArrayList<Byte> answers = new ArrayList<>();
        if (villageId <= 0 || playerId <= 0) {
            return answers;
        }
        String sql =
            "SELECT answer FROM point_walk_answers " +
            "WHERE village_id = ? AND player_id = ? ORDER BY building_id";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, villageId);
            statement.setInt(2, playerId);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    answers.add((byte) rs.getInt("answer"));
                }
            }
        } catch (SQLException ignored) {
            // Keep empty fallback.
        }
        return answers;
    }

    public synchronized ArrayList<PointWalkHighscoreRecord> loadPointWalkVillageHighscore(int villageId, int limit) {
        ArrayList<PointWalkHighscoreRecord> records = new ArrayList<>();
        if (villageId <= 0) {
            return records;
        }
        int safeLimit = limit > 0 ? Math.min(100, limit) : 20;
        String sql =
            "SELECT p.player_id, COALESCE(u.username, '') AS player_name, " +
            "SUM(CASE WHEN p.was_correct <> 0 THEN 1 ELSE 0 END) AS score " +
            "FROM point_walk_answers p " +
            "LEFT JOIN users u ON u.user_id = p.player_id " +
            "WHERE p.village_id = ? " +
            "GROUP BY p.player_id, player_name " +
            "ORDER BY score DESC, p.player_id ASC " +
            "LIMIT ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, villageId);
            statement.setInt(2, safeLimit);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    int playerId = rs.getInt("player_id");
                    String playerName = safe(rs.getString("player_name"));
                    if (playerName.isEmpty()) {
                        playerName = "Player " + playerId;
                    }
                    records.add(new PointWalkHighscoreRecord(playerId, playerName, rs.getInt("score")));
                }
            }
        } catch (SQLException ignored) {
            // Keep empty fallback.
        }
        return records;
    }

    public synchronized long addEvent(int player1Id, int player2Id, short type, byte[] data) throws IOException {
        if (player1Id <= 0) {
            return 0L;
        }
        String sql =
            "INSERT INTO player_events (player1_id, player2_id, event_type, data, created_at) VALUES (?, ?, ?, ?, ?)";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            statement.setInt(1, player1Id);
            statement.setInt(2, Math.max(0, player2Id));
            statement.setInt(3, type);
            statement.setBytes(4, nullableBytes(data));
            statement.setLong(5, System.currentTimeMillis());
            statement.executeUpdate();
            try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    return generatedKeys.getLong(1);
                }
            }
            return 0L;
        } catch (SQLException e) {
            throw new IOException("Failed to add event", e);
        }
    }

    public synchronized EventPage loadEventsForPlayer(int playerId, int offset, int amount) {
        int safeOffset = Math.max(0, offset);
        int safeAmount = amount > 0 ? Math.min(100, amount) : 20;
        ArrayList<Event> events = new ArrayList<>();
        if (playerId <= 0) {
            return new EventPage(events, false);
        }

        int total = 0;
        String countSql = "SELECT COUNT(*) FROM player_events WHERE player1_id = ? OR player2_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(countSql)) {
            statement.setInt(1, playerId);
            statement.setInt(2, playerId);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    total = rs.getInt(1);
                }
            }
        } catch (SQLException ignored) {
            // Keep empty fallback.
        }

        String sql =
            "SELECT e.event_id, e.player1_id, e.player2_id, e.event_type, e.data, e.created_at, " +
            "COALESCE(u1.username, '') AS player1_name, COALESCE(u2.username, '') AS player2_name " +
            "FROM player_events e " +
            "LEFT JOIN users u1 ON u1.user_id = e.player1_id " +
            "LEFT JOIN users u2 ON u2.user_id = e.player2_id " +
            "WHERE e.player1_id = ? OR e.player2_id = ? " +
            "ORDER BY e.created_at DESC, e.event_id DESC LIMIT ? OFFSET ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            statement.setInt(2, playerId);
            statement.setInt(3, safeAmount);
            statement.setInt(4, safeOffset);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    int player1Id = rs.getInt("player1_id");
                    int player2Id = rs.getInt("player2_id");
                    String player1Name = safe(rs.getString("player1_name"));
                    String player2Name = safe(rs.getString("player2_name"));
                    if (player1Name.isEmpty()) {
                        player1Name = "Player " + player1Id;
                    }
                    if (player2Id > 0 && player2Name.isEmpty()) {
                        player2Name = "Player " + player2Id;
                    }
                    events.add(
                        new Event(
                            rs.getLong("event_id"),
                            player1Id,
                            player2Id,
                            (short) rs.getInt("event_type"),
                            nullableBytes(rs.getBytes("data")),
                            rs.getLong("created_at"),
                            player1Name,
                            player2Name
                        )
                    );
                }
            }
        } catch (SQLException ignored) {
            // Keep empty fallback.
        }

        boolean olderEventsAvailable = safeOffset + events.size() < total;
        return new EventPage(events, olderEventsAvailable);
    }

    public synchronized long storeShare(int playerId, short shareType, long shareData) throws IOException {
        if (playerId <= 0) {
            return 0L;
        }
        String sql =
            "INSERT INTO shares (player_id, share_type, share_data, created_at) VALUES (?, ?, ?, ?)";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            statement.setInt(1, playerId);
            statement.setInt(2, shareType);
            statement.setLong(3, shareData);
            statement.setLong(4, System.currentTimeMillis());
            statement.executeUpdate();
            try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    return generatedKeys.getLong(1);
                }
            }
            return 0L;
        } catch (SQLException e) {
            throw new IOException("Failed to store share", e);
        }
    }

    public synchronized ArrayList<Integer> loadUploadedPictureIds(int playerId, int limit) {
        int safeLimit = limit > 0 ? Math.min(256, limit) : 32;
        ArrayList<Integer> ids = new ArrayList<>();
        if (playerId <= 0) {
            return ids;
        }
        String sql =
            "SELECT id FROM uploaded_pictures " +
            "WHERE player_id = ? " +
            "ORDER BY created_at DESC, id DESC " +
            "LIMIT ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            statement.setInt(2, safeLimit);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getInt("id"));
                }
            }
        } catch (SQLException ignored) {
            // Keep empty fallback.
        }
        return ids;
    }

    public synchronized int loadLatestUploadedPictureId(int playerId) {
        if (playerId <= 0) {
            return 0;
        }
        String sql =
            "SELECT id FROM uploaded_pictures " +
            "WHERE player_id = ? AND checksum_valid <> 0 " +
            "ORDER BY created_at DESC, id DESC " +
            "LIMIT 1";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("id");
                }
            }
        } catch (SQLException ignored) {
            // Keep empty fallback.
        }
        return 0;
    }

    public synchronized byte[] loadUploadedPictureBytes(long imageId) {
        return loadUploadedPictureBytes(imageId, false);
    }

    public synchronized byte[] loadUploadedPictureBytes(long imageId, boolean thumbnail) {
        if (imageId <= 0L || imageId > Integer.MAX_VALUE) {
            return new byte[0];
        }
        String sql = "SELECT image_path, thumbnail_path FROM uploaded_pictures WHERE id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, imageId);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    return new byte[0];
                }
                String imagePath = safe(rs.getString("image_path")).trim();
                String thumbnailPath = safe(rs.getString("thumbnail_path")).trim();
                if (thumbnail) {
                    byte[] thumbnailBytes = readUploadedPictureBytesFromPath(thumbnailPath);
                    if (thumbnailBytes.length > 0) {
                        return thumbnailBytes;
                    }
                }
                return readUploadedPictureBytesFromPath(imagePath);
            }
        } catch (SQLException ignored) {
            return new byte[0];
        }
    }

    private byte[] readUploadedPictureBytesFromPath(String picturePath) {
        String imagePath = safe(picturePath).trim();
        if (imagePath.isEmpty()) {
            return new byte[0];
        }
        try {
            Path resolvedPath = Paths.get(imagePath);
            if (!resolvedPath.isAbsolute()) {
                Path baseDir = dbPath.toAbsolutePath().getParent();
                if (baseDir != null) {
                    resolvedPath = baseDir.resolve(imagePath);
                }
            }
            resolvedPath = resolvedPath.toAbsolutePath().normalize();
            if (!Files.isRegularFile(resolvedPath)) {
                return new byte[0];
            }
            return nullableBytes(Files.readAllBytes(resolvedPath));
        } catch (IOException ignored) {
            return new byte[0];
        }
    }

    public synchronized void storeTwitterTokens(int playerId, String token, String tokenSecret, int sessionId) throws IOException {
        if (playerId <= 0) {
            return;
        }
        String sql =
            "INSERT INTO twitter_tokens (player_id, token, token_secret, session_id, updated_at) VALUES (?, ?, ?, ?, ?) " +
            "ON CONFLICT(player_id) DO UPDATE SET " +
            "token = excluded.token, " +
            "token_secret = excluded.token_secret, " +
            "session_id = excluded.session_id, " +
            "updated_at = excluded.updated_at";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            statement.setString(2, safe(token));
            statement.setString(3, safe(tokenSecret));
            statement.setInt(4, Math.max(0, sessionId));
            statement.setLong(5, System.currentTimeMillis());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to store twitter tokens", e);
        }
    }

    public synchronized TwitterTokensRecord loadTwitterTokens(int playerId) {
        if (playerId <= 0) {
            return null;
        }
        String sql =
            "SELECT player_id, token, token_secret, session_id, updated_at " +
            "FROM twitter_tokens WHERE player_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new TwitterTokensRecord(
                    rs.getInt("player_id"),
                    safe(rs.getString("token")),
                    safe(rs.getString("token_secret")),
                    rs.getInt("session_id"),
                    rs.getLong("updated_at")
                );
            }
        } catch (SQLException ignored) {
            return null;
        }
    }

    public synchronized void storeServerSessionParameters(int playerId,
                                                          int sessionId,
                                                          String system,
                                                          String javaVersion,
                                                          String adapter,
                                                          String displayVendor,
                                                          String displayRenderer,
                                                          String displayDriverVersion,
                                                          String displayApiVersion) throws IOException {
        if (playerId <= 0 || sessionId <= 0) {
            return;
        }
        String sql =
            "INSERT INTO server_session_parameters (" +
            "player_id, session_id, system_name, java_version, adapter, display_vendor, display_renderer, " +
            "display_driver_version, display_api_version, created_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            statement.setInt(2, sessionId);
            statement.setString(3, safe(system));
            statement.setString(4, safe(javaVersion));
            statement.setString(5, safe(adapter));
            statement.setString(6, safe(displayVendor));
            statement.setString(7, safe(displayRenderer));
            statement.setString(8, safe(displayDriverVersion));
            statement.setString(9, safe(displayApiVersion));
            statement.setLong(10, System.currentTimeMillis());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to store server session parameters", e);
        }
    }

    public synchronized void storeReport(int reporterPlayerId, int reportedPlayerId, int type, byte[] data) throws IOException {
        if (reporterPlayerId <= 0) {
            return;
        }
        String sql =
            "INSERT INTO reports (player_id, report_player_id, report_type, data, created_at) " +
            "VALUES (?, ?, ?, ?, ?)";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, reporterPlayerId);
            statement.setInt(2, Math.max(0, reportedPlayerId));
            statement.setInt(3, type);
            statement.setBytes(4, nullableBytes(data));
            statement.setLong(5, System.currentTimeMillis());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to store report", e);
        }
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
                "slx_credits INTEGER NOT NULL DEFAULT 3000, " +
                "last_rewarded_level INTEGER NOT NULL DEFAULT -1" +
                ")"
            );
            statement.execute(
                "CREATE TABLE IF NOT EXISTS account_profiles (" +
                "player_id INTEGER PRIMARY KEY, " +
                "first_name TEXT NOT NULL DEFAULT '', " +
                "sur_name TEXT NOT NULL DEFAULT '', " +
                "email TEXT NOT NULL DEFAULT '', " +
                "city TEXT NOT NULL DEFAULT '', " +
                "presentation_picture_url TEXT NOT NULL DEFAULT '', " +
                "country_id INTEGER NOT NULL DEFAULT 0, " +
                "birth_date INTEGER NOT NULL DEFAULT 0, " +
                "birth_month INTEGER NOT NULL DEFAULT 0, " +
                "birth_year INTEGER NOT NULL DEFAULT 0, " +
                "sex INTEGER NOT NULL DEFAULT 0, " +
                "wants_email INTEGER NOT NULL DEFAULT 0, " +
                "referral_id INTEGER NOT NULL DEFAULT 0, " +
                "fb_user INTEGER NOT NULL DEFAULT 0, " +
                "access_token TEXT NOT NULL DEFAULT '', " +
                "created_at INTEGER NOT NULL DEFAULT 0, " +
                "FOREIGN KEY(player_id) REFERENCES users(user_id) ON DELETE CASCADE" +
                ")"
            );
            try {
                statement.execute("ALTER TABLE users ADD COLUMN heightmap_preset_id INTEGER NOT NULL DEFAULT 0");
            } catch (SQLException ignored) {
                // Column already exists on upgraded databases.
            }
            try {
                statement.execute("ALTER TABLE users ADD COLUMN slx_credits INTEGER NOT NULL DEFAULT 3000");
            } catch (SQLException ignored) {
                // Column already exists on upgraded databases.
            }
            try {
                statement.execute("ALTER TABLE users ADD COLUMN last_rewarded_level INTEGER NOT NULL DEFAULT -1");
            } catch (SQLException ignored) {
                // Column already exists on upgraded databases.
            }
            // Normalize inconsistent historical states where last_rewarded_level
            // equals/exceeds current level, which blocks next level reward grants.
            statement.executeUpdate(
                "UPDATE users " +
                "SET last_rewarded_level = CASE " +
                "WHEN level > 0 THEN level - 1 " +
                "ELSE 0 END " +
                "WHERE last_rewarded_level >= level"
            );
            try {
                statement.execute("ALTER TABLE account_profiles ADD COLUMN city TEXT NOT NULL DEFAULT ''");
            } catch (SQLException ignored) {
                // Column already exists on upgraded databases.
            }
            try {
                statement.execute("ALTER TABLE account_profiles ADD COLUMN presentation_picture_url TEXT NOT NULL DEFAULT ''");
            } catch (SQLException ignored) {
                // Column already exists on upgraded databases.
            }
            try {
                statement.execute("ALTER TABLE account_profiles ADD COLUMN wants_email INTEGER NOT NULL DEFAULT 0");
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
                "CREATE TABLE IF NOT EXISTS simulated_characters (" +
                "village_id INTEGER NOT NULL, " +
                "character_id INTEGER NOT NULL, " +
                "pos_x INTEGER NOT NULL DEFAULT 0, " +
                "pos_z INTEGER NOT NULL DEFAULT 0, " +
                "family_id INTEGER NOT NULL DEFAULT 0, " +
                "home_id INTEGER NOT NULL DEFAULT 0, " +
                "data BLOB, " +
                "in_building_id INTEGER NOT NULL DEFAULT 0, " +
                "type INTEGER NOT NULL DEFAULT 0, " +
                "first_name TEXT NOT NULL DEFAULT '', " +
                "sur_name TEXT NOT NULL DEFAULT '', " +
                "consumed INTEGER NOT NULL DEFAULT 0, " +
                "PRIMARY KEY(village_id, character_id)" +
                ")"
            );

            statement.execute(
                "CREATE TABLE IF NOT EXISTS village_heightmaps (" +
                "village_id INTEGER PRIMARY KEY, " +
                "height_data BLOB NOT NULL, " +
                "type INTEGER NOT NULL DEFAULT 0, " +
                "size INTEGER NOT NULL DEFAULT 0" +
                ")"
            );
            statement.execute("INSERT OR IGNORE INTO counters(name, value) VALUES ('village_heightmap_bounds_backfill_done', 0)");
            int villageHeightMapBackfillDone = 0;
            try (ResultSet rs = statement.executeQuery(
                "SELECT value FROM counters WHERE name = 'village_heightmap_bounds_backfill_done'"
            )) {
                if (rs.next()) {
                    villageHeightMapBackfillDone = rs.getInt(1);
                }
            }
            if (villageHeightMapBackfillDone == 0) {
                int inserted = statement.executeUpdate(
                    "INSERT INTO village_heightmaps (village_id, height_data, type, size) " +
                    "SELECT users.village_id, X'', " + DEFAULT_VILLAGE_THEME + ", " + DEFAULT_VILLAGE_BOUNDS_SIZE + " " +
                    "FROM users " +
                    "LEFT JOIN village_heightmaps ON village_heightmaps.village_id = users.village_id " +
                    "WHERE village_heightmaps.village_id IS NULL"
                );
                int repaired = statement.executeUpdate(
                    "UPDATE village_heightmaps SET " +
                    "type = CASE WHEN type < 0 THEN " + DEFAULT_VILLAGE_THEME + " ELSE type END, " +
                    "size = CASE WHEN size <= 0 THEN " + DEFAULT_VILLAGE_BOUNDS_SIZE + " ELSE size END " +
                    "WHERE type < 0 OR size <= 0"
                );
                if (inserted > 0 || repaired > 0) {
                    System.out.println(
                        "[Server] Backfilled village heightmaps: inserted=" + inserted +
                        " repaired=" + repaired
                    );
                }
                statement.executeUpdate(
                    "UPDATE counters SET value = 1 WHERE name = 'village_heightmap_bounds_backfill_done'"
                );
            }

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
                "CREATE TABLE IF NOT EXISTS companion_ownership (" +
                "player_id INTEGER NOT NULL, " +
                "companion_kind TEXT NOT NULL, " +
                "companion_type INTEGER NOT NULL, " +
                "acquired_at INTEGER NOT NULL DEFAULT 0, " +
                "source TEXT NOT NULL DEFAULT '', " +
                "PRIMARY KEY(player_id, companion_kind, companion_type), " +
                "FOREIGN KEY(player_id) REFERENCES users(user_id) ON DELETE CASCADE" +
                ")"
            );
            statement.execute(
                "CREATE INDEX IF NOT EXISTS idx_companion_ownership_player " +
                "ON companion_ownership(player_id, companion_kind, companion_type)"
            );
            statement.execute(
                "CREATE TABLE IF NOT EXISTS player_clothes (" +
                "player_id INTEGER NOT NULL, " +
                "cloth_id INTEGER NOT NULL, " +
                "hue REAL NOT NULL DEFAULT 0, " +
                "saturation REAL NOT NULL DEFAULT 0, " +
                "brightness REAL NOT NULL DEFAULT 0, " +
                "opacity REAL NOT NULL DEFAULT 0, " +
                "PRIMARY KEY(player_id, cloth_id)" +
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
                "CREATE TABLE IF NOT EXISTS wheel_resource_state (" +
                "player_id INTEGER PRIMARY KEY, " +
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
            statement.execute(
                "CREATE TABLE IF NOT EXISTS chat_command_permissions (" +
                "command_name TEXT PRIMARY KEY, " +
                "required_role TEXT NOT NULL DEFAULT 'mod', " +
                "updated_at INTEGER NOT NULL DEFAULT 0" +
                ")"
            );
            statement.execute(
                "INSERT OR IGNORE INTO chat_command_permissions(command_name, required_role, updated_at) " +
                "VALUES ('setcoins', 'mod', 0)"
            );
            statement.execute(
                "INSERT OR IGNORE INTO chat_command_permissions(command_name, required_role, updated_at) " +
                "VALUES ('setlevel', 'mod', 0)"
            );
            statement.executeUpdate(
                "UPDATE chat_command_permissions " +
                "SET required_role = 'mod' " +
                "WHERE lower(required_role) NOT IN ('everyone', 'vip', 'mod')"
            );
            statement.execute(
                "CREATE TABLE IF NOT EXISTS admin_popup_queue (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "sender_name TEXT NOT NULL DEFAULT 'update', " +
                "message_text TEXT NOT NULL DEFAULT '', " +
                "created_at INTEGER NOT NULL DEFAULT 0, " +
                "delivered_at INTEGER NOT NULL DEFAULT 0" +
                ")"
            );
            statement.execute(
                "CREATE INDEX IF NOT EXISTS idx_admin_popup_queue_pending ON admin_popup_queue(delivered_at, id)"
            );

            statement.execute(
                "CREATE TABLE IF NOT EXISTS guestbook_entries (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "owner_player_id INTEGER NOT NULL, " +
                "poster_player_id INTEGER NOT NULL, " +
                "post_text TEXT NOT NULL DEFAULT '', " +
                "is_private INTEGER NOT NULL DEFAULT 0, " +
                "created_at INTEGER NOT NULL DEFAULT 0, " +
                "post_type TEXT NOT NULL DEFAULT ''" +
                ")"
            );
            statement.execute("CREATE INDEX IF NOT EXISTS idx_guestbook_owner_created ON guestbook_entries(owner_player_id, created_at DESC, id DESC)");
            statement.execute("CREATE INDEX IF NOT EXISTS idx_guestbook_pair_created ON guestbook_entries(owner_player_id, poster_player_id, created_at DESC, id DESC)");

            statement.execute(
                "CREATE TABLE IF NOT EXISTS guestbook_state (" +
                "player_id INTEGER PRIMARY KEY, " +
                "new_entries_available INTEGER NOT NULL DEFAULT 0" +
                ")"
            );

            statement.execute(
                "CREATE TABLE IF NOT EXISTS midi_songs (" +
                "song_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "owner_player_id INTEGER NOT NULL, " +
                "is_gift INTEGER NOT NULL DEFAULT 0, " +
                "data BLOB NOT NULL, " +
                "created_at INTEGER NOT NULL DEFAULT 0" +
                ")"
            );
            statement.execute("CREATE INDEX IF NOT EXISTS idx_midi_owner_created ON midi_songs(owner_player_id, created_at DESC, song_id DESC)");

            statement.execute(
                "CREATE TABLE IF NOT EXISTS quests (" +
                "quest_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "player_id INTEGER NOT NULL, " +
                "target_player_id INTEGER NOT NULL DEFAULT 0, " +
                "quest_type INTEGER NOT NULL, " +
                "data BLOB, " +
                "finished INTEGER NOT NULL DEFAULT 0, " +
                "created_at INTEGER NOT NULL DEFAULT 0, " +
                "updated_at INTEGER NOT NULL DEFAULT 0" +
                ")"
            );
            statement.execute("CREATE INDEX IF NOT EXISTS idx_quests_player ON quests(player_id, quest_id DESC)");
            statement.execute("CREATE INDEX IF NOT EXISTS idx_quests_player_type ON quests(player_id, quest_type, finished)");

            statement.execute(
                "CREATE TABLE IF NOT EXISTS point_walk_answers (" +
                "village_id INTEGER NOT NULL, " +
                "player_id INTEGER NOT NULL, " +
                "building_id INTEGER NOT NULL, " +
                "answer INTEGER NOT NULL DEFAULT 0, " +
                "was_correct INTEGER NOT NULL DEFAULT 0, " +
                "created_at INTEGER NOT NULL DEFAULT 0, " +
                "updated_at INTEGER NOT NULL DEFAULT 0, " +
                "PRIMARY KEY(village_id, player_id, building_id)" +
                ")"
            );
            statement.execute("CREATE INDEX IF NOT EXISTS idx_pointwalk_village_building ON point_walk_answers(village_id, building_id)");
            statement.execute("CREATE INDEX IF NOT EXISTS idx_pointwalk_village_player ON point_walk_answers(village_id, player_id)");

            statement.execute(
                "CREATE TABLE IF NOT EXISTS player_events (" +
                "event_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "player1_id INTEGER NOT NULL, " +
                "player2_id INTEGER NOT NULL DEFAULT 0, " +
                "event_type INTEGER NOT NULL, " +
                "data BLOB, " +
                "created_at INTEGER NOT NULL DEFAULT 0" +
                ")"
            );
            statement.execute("CREATE INDEX IF NOT EXISTS idx_player_events_player1 ON player_events(player1_id, created_at DESC, event_id DESC)");
            statement.execute("CREATE INDEX IF NOT EXISTS idx_player_events_player2 ON player_events(player2_id, created_at DESC, event_id DESC)");

            statement.execute(
                "CREATE TABLE IF NOT EXISTS shares (" +
                "share_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "player_id INTEGER NOT NULL, " +
                "share_type INTEGER NOT NULL, " +
                "share_data INTEGER NOT NULL DEFAULT 0, " +
                "created_at INTEGER NOT NULL DEFAULT 0" +
                ")"
            );

            statement.execute(
                "CREATE TABLE IF NOT EXISTS uploaded_pictures (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "player_id INTEGER NOT NULL, " +
                "server_name TEXT NOT NULL DEFAULT '', " +
                "image_path TEXT NOT NULL DEFAULT '', " +
                "thumbnail_path TEXT NOT NULL DEFAULT '', " +
                "created_at INTEGER NOT NULL DEFAULT 0, " +
                "checksum_valid INTEGER NOT NULL DEFAULT 0" +
                ")"
            );
            try {
                statement.execute("ALTER TABLE uploaded_pictures ADD COLUMN thumbnail_path TEXT NOT NULL DEFAULT ''");
            } catch (SQLException ignored) {
                // Column already exists on upgraded databases.
            }
            statement.execute("CREATE INDEX IF NOT EXISTS idx_uploaded_pictures_player ON uploaded_pictures(player_id, created_at DESC, id DESC)");

            statement.execute(
                "CREATE TABLE IF NOT EXISTS reports (" +
                "report_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "player_id INTEGER NOT NULL, " +
                "report_player_id INTEGER NOT NULL DEFAULT 0, " +
                "report_type INTEGER NOT NULL DEFAULT 0, " +
                "data BLOB, " +
                "created_at INTEGER NOT NULL DEFAULT 0" +
                ")"
            );

            statement.execute(
                "CREATE TABLE IF NOT EXISTS blocked_players (" +
                "owner_player_id INTEGER NOT NULL, " +
                "blocked_player_id INTEGER NOT NULL, " +
                "created_at INTEGER NOT NULL DEFAULT 0, " +
                "PRIMARY KEY(owner_player_id, blocked_player_id), " +
                "FOREIGN KEY(owner_player_id) REFERENCES users(user_id) ON DELETE CASCADE, " +
                "FOREIGN KEY(blocked_player_id) REFERENCES users(user_id) ON DELETE CASCADE" +
                ")"
            );
            statement.execute("CREATE INDEX IF NOT EXISTS idx_blocked_players_owner ON blocked_players(owner_player_id, created_at DESC)");

            statement.execute(
                "CREATE TABLE IF NOT EXISTS login_blacklist (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "player_id INTEGER NOT NULL DEFAULT 0, " +
                "username TEXT NOT NULL DEFAULT '', " +
                "mac TEXT NOT NULL DEFAULT '', " +
                "message TEXT NOT NULL DEFAULT '', " +
                "blocked_until INTEGER NOT NULL DEFAULT 0, " +
                "created_at INTEGER NOT NULL DEFAULT 0" +
                ")"
            );
            statement.execute("CREATE INDEX IF NOT EXISTS idx_login_blacklist_player ON login_blacklist(player_id, blocked_until DESC, id DESC)");
            statement.execute("CREATE INDEX IF NOT EXISTS idx_login_blacklist_username ON login_blacklist(username, blocked_until DESC, id DESC)");
            statement.execute("CREATE INDEX IF NOT EXISTS idx_login_blacklist_mac ON login_blacklist(mac, blocked_until DESC, id DESC)");

            statement.execute(
                "CREATE TABLE IF NOT EXISTS twitter_tokens (" +
                "player_id INTEGER PRIMARY KEY, " +
                "token TEXT NOT NULL DEFAULT '', " +
                "token_secret TEXT NOT NULL DEFAULT '', " +
                "session_id INTEGER NOT NULL DEFAULT 0, " +
                "updated_at INTEGER NOT NULL DEFAULT 0, " +
                "FOREIGN KEY(player_id) REFERENCES users(user_id) ON DELETE CASCADE" +
                ")"
            );

            statement.execute(
                "CREATE TABLE IF NOT EXISTS server_session_parameters (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "player_id INTEGER NOT NULL, " +
                "session_id INTEGER NOT NULL, " +
                "system_name TEXT NOT NULL DEFAULT '', " +
                "java_version TEXT NOT NULL DEFAULT '', " +
                "adapter TEXT NOT NULL DEFAULT '', " +
                "display_vendor TEXT NOT NULL DEFAULT '', " +
                "display_renderer TEXT NOT NULL DEFAULT '', " +
                "display_driver_version TEXT NOT NULL DEFAULT '', " +
                "display_api_version TEXT NOT NULL DEFAULT '', " +
                "created_at INTEGER NOT NULL DEFAULT 0, " +
                "FOREIGN KEY(player_id) REFERENCES users(user_id) ON DELETE CASCADE" +
                ")"
            );
            statement.execute("CREATE INDEX IF NOT EXISTS idx_server_session_parameters_player ON server_session_parameters(player_id, created_at DESC)");

            statement.execute(
                "CREATE TABLE IF NOT EXISTS im_list (" +
                "player_id INTEGER NOT NULL, " +
                "other_player_id INTEGER NOT NULL, " +
                "created_at INTEGER NOT NULL DEFAULT 0, " +
                "PRIMARY KEY(player_id, other_player_id), " +
                "FOREIGN KEY(player_id) REFERENCES users(user_id) ON DELETE CASCADE, " +
                "FOREIGN KEY(other_player_id) REFERENCES users(user_id) ON DELETE CASCADE" +
                ")"
            );
            statement.execute("CREATE INDEX IF NOT EXISTS idx_im_list_player ON im_list(player_id, created_at DESC)");
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

    private static String normalizeCompanionKind(String companionKind) {
        String normalized = safe(companionKind).trim().toLowerCase();
        if ("mount".equals(normalized) || "horse".equals(normalized)) {
            return "mount";
        }
        if ("pet".equals(normalized)) {
            return "pet";
        }
        return "";
    }

    private static String normalizeChatCommandPermission(String permission) {
        String normalized = safe(permission).trim().toLowerCase();
        if (CHAT_COMMAND_PERMISSION_EVERYONE.equals(normalized)) {
            return CHAT_COMMAND_PERMISSION_EVERYONE;
        }
        if (CHAT_COMMAND_PERMISSION_VIP.equals(normalized)) {
            return CHAT_COMMAND_PERMISSION_VIP;
        }
        if (CHAT_COMMAND_PERMISSION_MOD.equals(normalized)) {
            return CHAT_COMMAND_PERMISSION_MOD;
        }
        return CHAT_COMMAND_PERMISSION_MOD;
    }

    private static String normalizeChatCommandName(String commandName) {
        String normalized = safe(commandName).trim().toLowerCase();
        if (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        return normalized;
    }

    private static byte[] nullableBytes(byte[] data) {
        return data == null ? new byte[0] : data;
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

    private static boolean hasAllRequiredIngredientBits(long ingredients, int requiredIngredientCount) {
        if (requiredIngredientCount <= 0) {
            return true;
        }
        long requiredMask = 0L;
        for (int i = 0; i < requiredIngredientCount; i++) {
            if (i >= 63) {
                requiredMask = -1L;
                break;
            }
            requiredMask |= (1L << i);
        }
        return (ingredients & requiredMask) == requiredMask;
    }

    private static void bindUpsertBuildingStatement(PreparedStatement statement, int villageId, Building building) throws SQLException {
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

    public synchronized int setSLXCredits(int userId, int credits) throws IOException {
        int safeCredits = Math.max(0, credits);
        String sql = "UPDATE users SET slx_credits = ? WHERE user_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, safeCredits);
            statement.setInt(2, userId);
            statement.executeUpdate();
            return getSLXCredits(userId);
        } catch (SQLException e) {
            throw new IOException("Failed to set SLX credits", e);
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

    public synchronized LevelRewardResult applyCompletedLevelReward(int userId,
                                                                    int reportedCompletedLevel,
                                                                    int rewardBaseCredits,
                                                                    int rewardStepCredits) throws IOException {
        int safeCompletedLevel = Math.max(0, reportedCompletedLevel);
        int safeRewardBase = Math.max(0, rewardBaseCredits);
        int safeRewardStep = Math.max(0, rewardStepCredits);
        String selectSql = "SELECT level, slx_credits, last_rewarded_level FROM users WHERE user_id = ?";
        String updateSql = "UPDATE users SET level = ?, slx_credits = ?, last_rewarded_level = ? WHERE user_id = ?";

        try (Connection connection = openConnection()) {
            boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                int currentLevel = 1;
                int currentCredits = 0;
                int storedLastRewardedLevel = -1;
                boolean found = false;
                try (PreparedStatement selectStatement = connection.prepareStatement(selectSql)) {
                    selectStatement.setInt(1, userId);
                    try (ResultSet rs = selectStatement.executeQuery()) {
                        if (rs.next()) {
                            currentLevel = Math.max(1, rs.getInt("level"));
                            currentCredits = Math.max(0, rs.getInt("slx_credits"));
                            storedLastRewardedLevel = rs.getInt("last_rewarded_level");
                            found = true;
                        }
                    }
                }
                if (!found) {
                    connection.rollback();
                    return new LevelRewardResult(1, 1, 0, 0, 0);
                }

                int newLevel = Math.max(currentLevel, safeCompletedLevel + 1);
                int baselineRewardLevel = storedLastRewardedLevel;
                if (baselineRewardLevel < 0) {
                    // Migration fallback: assume current DB level has already been rewarded up through level-1.
                    baselineRewardLevel = Math.max(0, currentLevel - 1);
                }
                // Guard against inconsistent historical states where last_rewarded_level
                // drifted above (current level - 1), which would block legitimate rewards.
                if (baselineRewardLevel > currentLevel - 1) {
                    baselineRewardLevel = Math.max(0, currentLevel - 1);
                }

                int rewardFromLevel = baselineRewardLevel + 1;
                int rewardToLevel = safeCompletedLevel;
                long creditsAwardedLong = 0L;
                if (rewardToLevel >= rewardFromLevel) {
                    long rewardedLevelCount = (long) rewardToLevel - rewardFromLevel + 1L;
                    long levelSum = ((long) rewardFromLevel + rewardToLevel) * rewardedLevelCount / 2L;
                    creditsAwardedLong = rewardedLevelCount * safeRewardBase + levelSum * safeRewardStep;
                }
                int creditsAwarded = clampToInt(creditsAwardedLong);
                int creditsAfter = clampToInt((long) currentCredits + creditsAwarded);
                int newLastRewardedLevel = Math.max(baselineRewardLevel, safeCompletedLevel);

                try (PreparedStatement updateStatement = connection.prepareStatement(updateSql)) {
                    updateStatement.setInt(1, newLevel);
                    updateStatement.setInt(2, creditsAfter);
                    updateStatement.setInt(3, newLastRewardedLevel);
                    updateStatement.setInt(4, userId);
                    updateStatement.executeUpdate();
                }

                connection.commit();
                return new LevelRewardResult(currentLevel, newLevel, creditsAwarded, creditsAfter, newLastRewardedLevel);
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(previousAutoCommit);
            }
        } catch (SQLException e) {
            throw new IOException("Failed to apply completed level reward", e);
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

    public synchronized int setUserLevel(int userId, int level) throws IOException {
        int nextLevel = Math.max(1, level);
        String sql = "UPDATE users SET level = ? WHERE user_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, nextLevel);
            statement.setInt(2, userId);
            statement.executeUpdate();
            UserRecord user = findByUserId(userId);
            return user != null ? user.level : nextLevel;
        } catch (SQLException e) {
            throw new IOException("Failed to set user level", e);
        }
    }

    public synchronized String getChatCommandRequiredRole(String commandName) {
        String normalizedCommandName = normalizeChatCommandName(commandName);
        if (normalizedCommandName.isEmpty()) {
            return CHAT_COMMAND_PERMISSION_MOD;
        }
        String sql = "SELECT required_role FROM chat_command_permissions WHERE command_name = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, normalizedCommandName);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    return CHAT_COMMAND_PERMISSION_MOD;
                }
                return normalizeChatCommandPermission(rs.getString(1));
            }
        } catch (SQLException e) {
            return CHAT_COMMAND_PERMISSION_MOD;
        }
    }

    public synchronized void upsertChatCommandRequiredRole(String commandName, String requiredRole) throws IOException {
        String normalizedCommandName = normalizeChatCommandName(commandName);
        if (normalizedCommandName.isEmpty()) {
            throw new IOException("Command name cannot be empty");
        }
        String normalizedRequiredRole = normalizeChatCommandPermission(requiredRole);
        String sql =
            "INSERT INTO chat_command_permissions(command_name, required_role, updated_at) VALUES (?, ?, ?) " +
            "ON CONFLICT(command_name) DO UPDATE SET required_role = excluded.required_role, updated_at = excluded.updated_at";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, normalizedCommandName);
            statement.setString(2, normalizedRequiredRole);
            statement.setLong(3, System.currentTimeMillis());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to upsert chat command required role", e);
        }
    }

    public synchronized ArrayList<AdminPopupQueueRecord> loadPendingAdminPopupQueue(int limit) {
        int boundedLimit = Math.max(1, Math.min(256, limit));
        ArrayList<AdminPopupQueueRecord> rows = new ArrayList<>();
        String sql =
            "SELECT id, sender_name, message_text, created_at " +
            "FROM admin_popup_queue " +
            "WHERE delivered_at = 0 " +
            "ORDER BY id ASC " +
            "LIMIT ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, boundedLimit);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    rows.add(new AdminPopupQueueRecord(
                        rs.getLong("id"),
                        rs.getString("sender_name"),
                        rs.getString("message_text"),
                        rs.getLong("created_at")
                    ));
                }
            }
        } catch (SQLException ignored) {
            // Keep empty fallback.
        }
        return rows;
    }

    public synchronized void markAdminPopupQueueDelivered(long id) throws IOException {
        if (id <= 0L) {
            return;
        }
        String sql =
            "UPDATE admin_popup_queue " +
            "SET delivered_at = ? " +
            "WHERE id = ? AND delivered_at = 0";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, System.currentTimeMillis());
            statement.setLong(2, id);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to mark admin popup queue row delivered", e);
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

    public synchronized void storeWheelResourceState(int playerId,
                                                     boolean lit1, boolean lit2, boolean lit3, boolean lit4,
                                                     boolean lit5, boolean lit6, boolean lit7, boolean lit8,
                                                     short res1, short res2, short res3, short res4,
                                                     short res5, short res6, short res7, short res8) throws IOException {
        String sql =
            "INSERT INTO wheel_resource_state (" +
            "player_id, lit1, lit2, lit3, lit4, lit5, lit6, lit7, lit8, " +
            "res1, res2, res3, res4, res5, res6, res7, res8, updated_at" +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT(player_id) DO UPDATE SET " +
            "lit1=excluded.lit1, lit2=excluded.lit2, lit3=excluded.lit3, lit4=excluded.lit4, " +
            "lit5=excluded.lit5, lit6=excluded.lit6, lit7=excluded.lit7, lit8=excluded.lit8, " +
            "res1=excluded.res1, res2=excluded.res2, res3=excluded.res3, res4=excluded.res4, " +
            "res5=excluded.res5, res6=excluded.res6, res7=excluded.res7, res8=excluded.res8, " +
            "updated_at=excluded.updated_at";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            statement.setInt(2, lit1 ? 1 : 0);
            statement.setInt(3, lit2 ? 1 : 0);
            statement.setInt(4, lit3 ? 1 : 0);
            statement.setInt(5, lit4 ? 1 : 0);
            statement.setInt(6, lit5 ? 1 : 0);
            statement.setInt(7, lit6 ? 1 : 0);
            statement.setInt(8, lit7 ? 1 : 0);
            statement.setInt(9, lit8 ? 1 : 0);
            statement.setInt(10, res1);
            statement.setInt(11, res2);
            statement.setInt(12, res3);
            statement.setInt(13, res4);
            statement.setInt(14, res5);
            statement.setInt(15, res6);
            statement.setInt(16, res7);
            statement.setInt(17, res8);
            statement.setLong(18, System.currentTimeMillis());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to store wheel resource state", e);
        }
    }

    public synchronized WheelResourceStateRecord loadWheelResourceState(int playerId) {
        String sql =
            "SELECT lit1, lit2, lit3, lit4, lit5, lit6, lit7, lit8, " +
            "res1, res2, res3, res4, res5, res6, res7, res8, updated_at " +
            "FROM wheel_resource_state WHERE player_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new WheelResourceStateRecord(
                    rs.getInt("lit1") != 0,
                    rs.getInt("lit2") != 0,
                    rs.getInt("lit3") != 0,
                    rs.getInt("lit4") != 0,
                    rs.getInt("lit5") != 0,
                    rs.getInt("lit6") != 0,
                    rs.getInt("lit7") != 0,
                    rs.getInt("lit8") != 0,
                    (short) rs.getInt("res1"),
                    (short) rs.getInt("res2"),
                    (short) rs.getInt("res3"),
                    (short) rs.getInt("res4"),
                    (short) rs.getInt("res5"),
                    (short) rs.getInt("res6"),
                    (short) rs.getInt("res7"),
                    (short) rs.getInt("res8"),
                    rs.getLong("updated_at")
                );
            }
        } catch (SQLException e) {
            return null;
        }
    }

    public synchronized SpinStateRecord loadSpinState(int playerId) {
        String sql =
            "SELECT level, wins, won, " +
            "lit1, lit2, lit3, lit4, lit5, lit6, lit7, lit8, " +
            "res1, res2, res3, res4, res5, res6, res7, res8, updated_at " +
            "FROM spin_state WHERE player_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new SpinStateRecord(
                    (short) rs.getInt("level"),
                    (short) rs.getInt("wins"),
                    rs.getInt("won") != 0,
                    rs.getInt("lit1") != 0,
                    rs.getInt("lit2") != 0,
                    rs.getInt("lit3") != 0,
                    rs.getInt("lit4") != 0,
                    rs.getInt("lit5") != 0,
                    rs.getInt("lit6") != 0,
                    rs.getInt("lit7") != 0,
                    rs.getInt("lit8") != 0,
                    (short) rs.getInt("res1"),
                    (short) rs.getInt("res2"),
                    (short) rs.getInt("res3"),
                    (short) rs.getInt("res4"),
                    (short) rs.getInt("res5"),
                    (short) rs.getInt("res6"),
                    (short) rs.getInt("res7"),
                    (short) rs.getInt("res8"),
                    rs.getLong("updated_at")
                );
            }
        } catch (SQLException e) {
            return null;
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

    public synchronized GuestbookPage loadGuestbookEntries(int ownerPlayerId, int viewerPlayerId, int offset, int limit) {
        int safeOffset = Math.max(0, offset);
        int safeLimit = limit > 0 ? Math.min(100, limit) : 20;
        int total = countVisibleGuestbookEntries(ownerPlayerId, viewerPlayerId);

        ArrayList<GuestbookEntryRecord> entries = new ArrayList<>();
        String sql =
            "SELECT g.id, g.owner_player_id, g.poster_player_id, g.post_text, g.is_private, g.created_at, g.post_type, " +
            "u.username AS poster_name, u.level AS poster_level " +
            "FROM guestbook_entries g " +
            "LEFT JOIN users u ON u.user_id = g.poster_player_id " +
            "WHERE g.owner_player_id = ? " +
            "AND (g.is_private = 0 OR g.owner_player_id = ? OR g.poster_player_id = ?) " +
            "ORDER BY g.created_at DESC, g.id DESC LIMIT ? OFFSET ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, ownerPlayerId);
            statement.setInt(2, viewerPlayerId);
            statement.setInt(3, viewerPlayerId);
            statement.setInt(4, safeLimit);
            statement.setInt(5, safeOffset);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    entries.add(mapGuestbookEntry(rs));
                }
            }
        } catch (SQLException ignored) {
            // Keep empty fallback.
        }

        boolean hasNewer = safeOffset > 0 && total > 0;
        boolean hasOlder = safeOffset + entries.size() < total;
        return new GuestbookPage(hasNewer, hasOlder, entries);
    }

    public synchronized GuestbookPage loadGuestbookHistory(int player1, int player2, int viewerPlayerId, int offset, int limit) {
        int safeOffset = Math.max(0, offset);
        int safeLimit = limit > 0 ? Math.min(100, limit) : 20;
        int total = countVisibleGuestbookHistory(player1, player2, viewerPlayerId);

        ArrayList<GuestbookEntryRecord> entries = new ArrayList<>();
        String sql =
            "SELECT g.id, g.owner_player_id, g.poster_player_id, g.post_text, g.is_private, g.created_at, g.post_type, " +
            "u.username AS poster_name, u.level AS poster_level " +
            "FROM guestbook_entries g " +
            "LEFT JOIN users u ON u.user_id = g.poster_player_id " +
            "WHERE ((g.owner_player_id = ? AND g.poster_player_id = ?) OR (g.owner_player_id = ? AND g.poster_player_id = ?)) " +
            "AND (g.is_private = 0 OR g.owner_player_id = ? OR g.poster_player_id = ?) " +
            "ORDER BY g.created_at DESC, g.id DESC LIMIT ? OFFSET ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, player1);
            statement.setInt(2, player2);
            statement.setInt(3, player2);
            statement.setInt(4, player1);
            statement.setInt(5, viewerPlayerId);
            statement.setInt(6, viewerPlayerId);
            statement.setInt(7, safeLimit);
            statement.setInt(8, safeOffset);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    entries.add(mapGuestbookEntry(rs));
                }
            }
        } catch (SQLException ignored) {
            // Keep empty fallback.
        }

        boolean hasNewer = safeOffset > 0 && total > 0;
        boolean hasOlder = safeOffset + entries.size() < total;
        return new GuestbookPage(hasNewer, hasOlder, entries);
    }

    public synchronized int addGuestbookEntry(int ownerPlayerId, int posterPlayerId, String text, boolean isPrivate) throws IOException {
        String normalizedText = safe(text).trim();
        if (ownerPlayerId <= 0 || posterPlayerId <= 0 || normalizedText.isEmpty()) {
            return 0;
        }

        String sql =
            "INSERT INTO guestbook_entries (owner_player_id, poster_player_id, post_text, is_private, created_at, post_type) " +
            "VALUES (?, ?, ?, ?, ?, '')";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            statement.setInt(1, ownerPlayerId);
            statement.setInt(2, posterPlayerId);
            statement.setString(3, normalizedText);
            statement.setInt(4, isPrivate ? 1 : 0);
            statement.setLong(5, System.currentTimeMillis());
            statement.executeUpdate();

            int createdPostId = 0;
            try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    createdPostId = generatedKeys.getInt(1);
                }
            }

            if (ownerPlayerId != posterPlayerId) {
                setAreNewGuestbookEntriesAvailable(ownerPlayerId, true);
            }
            return createdPostId;
        } catch (SQLException e) {
            throw new IOException("Failed to add guestbook entry", e);
        }
    }

    public synchronized boolean removeGuestbookEntry(int postId, int requesterPlayerId) throws IOException {
        if (postId <= 0 || requesterPlayerId <= 0) {
            return false;
        }
        String sql =
            "DELETE FROM guestbook_entries " +
            "WHERE id = ? AND (owner_player_id = ? OR poster_player_id = ?)";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, postId);
            statement.setInt(2, requesterPlayerId);
            statement.setInt(3, requesterPlayerId);
            return statement.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IOException("Failed to remove guestbook entry", e);
        }
    }

    public synchronized boolean setGuestbookEntryPrivate(int ownerPlayerId, int postId, int requesterPlayerId, boolean setPrivate) throws IOException {
        if (postId <= 0 || requesterPlayerId <= 0) {
            return false;
        }
        String sql =
            "UPDATE guestbook_entries SET is_private = ? " +
            "WHERE id = ? " +
            "AND (owner_player_id = ? OR ? = 0) " +
            "AND (owner_player_id = ? OR poster_player_id = ?)";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, setPrivate ? 1 : 0);
            statement.setInt(2, postId);
            statement.setInt(3, ownerPlayerId);
            statement.setInt(4, ownerPlayerId);
            statement.setInt(5, requesterPlayerId);
            statement.setInt(6, requesterPlayerId);
            return statement.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IOException("Failed to update guestbook privacy", e);
        }
    }

    public synchronized void addBlockedPlayer(int ownerPlayerId, int blockedPlayerId) throws IOException {
        if (ownerPlayerId <= 0 || blockedPlayerId <= 0 || ownerPlayerId == blockedPlayerId) {
            return;
        }
        String sql =
            "INSERT INTO blocked_players (owner_player_id, blocked_player_id, created_at) VALUES (?, ?, ?) " +
            "ON CONFLICT(owner_player_id, blocked_player_id) DO NOTHING";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, ownerPlayerId);
            statement.setInt(2, blockedPlayerId);
            statement.setLong(3, System.currentTimeMillis());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to add blocked player", e);
        }
    }

    public synchronized LoginBlacklistRecord loadActiveLoginBlacklist(int playerId, String username, String mac) {
        String normalizedUsername = normalizeUsername(username);
        String normalizedMac = safe(mac).trim();
        if (playerId <= 0 && normalizedUsername.isEmpty() && normalizedMac.isEmpty()) {
            return null;
        }
        long now = System.currentTimeMillis();
        String sql =
            "SELECT message, blocked_until " +
            "FROM login_blacklist " +
            "WHERE ((? > 0 AND player_id = ?) " +
            "OR (username <> '' AND lower(username) = lower(?)) " +
            "OR (mac <> '' AND mac = ?)) " +
            "AND (blocked_until <= 0 OR blocked_until > ?) " +
            "ORDER BY " +
            "CASE " +
            "WHEN (? > 0 AND player_id = ?) THEN 0 " +
            "WHEN (username <> '' AND lower(username) = lower(?)) THEN 1 " +
            "ELSE 2 END, " +
            "CASE WHEN blocked_until <= 0 THEN 1 ELSE 0 END DESC, " +
            "blocked_until DESC, " +
            "id DESC " +
            "LIMIT 1";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            statement.setInt(2, playerId);
            statement.setString(3, normalizedUsername);
            statement.setString(4, normalizedMac);
            statement.setLong(5, now);
            statement.setInt(6, playerId);
            statement.setInt(7, playerId);
            statement.setString(8, normalizedUsername);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return new LoginBlacklistRecord(
                        rs.getString("message"),
                        rs.getLong("blocked_until")
                    );
                }
            }
        } catch (SQLException ignored) {
            // Keep null fallback.
        }
        return null;
    }

    public synchronized boolean removeBlockedPlayer(int ownerPlayerId, int blockedPlayerId) throws IOException {
        if (ownerPlayerId <= 0 || blockedPlayerId <= 0) {
            return false;
        }
        String sql = "DELETE FROM blocked_players WHERE owner_player_id = ? AND blocked_player_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, ownerPlayerId);
            statement.setInt(2, blockedPlayerId);
            return statement.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IOException("Failed to remove blocked player", e);
        }
    }

    public synchronized boolean isPlayerBlocked(int ownerPlayerId, int blockedPlayerId) {
        if (ownerPlayerId <= 0 || blockedPlayerId <= 0) {
            return false;
        }
        String sql =
            "SELECT 1 FROM blocked_players WHERE owner_player_id = ? AND blocked_player_id = ? LIMIT 1";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, ownerPlayerId);
            statement.setInt(2, blockedPlayerId);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException ignored) {
            return false;
        }
    }

    public synchronized ArrayList<Integer> loadBlockedPlayerIds(int ownerPlayerId) {
        ArrayList<Integer> blockedIds = new ArrayList<>();
        if (ownerPlayerId <= 0) {
            return blockedIds;
        }
        String sql =
            "SELECT b.blocked_player_id " +
            "FROM blocked_players b " +
            "LEFT JOIN users u ON u.user_id = b.blocked_player_id " +
            "WHERE b.owner_player_id = ? " +
            "ORDER BY lower(COALESCE(u.username, '')), b.blocked_player_id";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, ownerPlayerId);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    blockedIds.add(rs.getInt(1));
                }
            }
        } catch (SQLException ignored) {
            // Keep empty fallback.
        }
        return blockedIds;
    }

    public synchronized void addIMListEntry(int playerId, int otherPlayerId) throws IOException {
        if (playerId <= 0 || otherPlayerId <= 0 || playerId == otherPlayerId) {
            return;
        }
        String sql =
            "INSERT INTO im_list (player_id, other_player_id, created_at) VALUES (?, ?, ?) " +
            "ON CONFLICT(player_id, other_player_id) DO NOTHING";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            statement.setInt(2, otherPlayerId);
            statement.setLong(3, System.currentTimeMillis());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to add IM list entry", e);
        }
    }

    public synchronized boolean removeIMListEntry(int playerId, int otherPlayerId) throws IOException {
        if (playerId <= 0 || otherPlayerId <= 0) {
            return false;
        }
        String sql = "DELETE FROM im_list WHERE player_id = ? AND other_player_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            statement.setInt(2, otherPlayerId);
            return statement.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IOException("Failed to remove IM list entry", e);
        }
    }

    public synchronized ArrayList<Integer> loadIMListPlayerIds(int playerId) {
        ArrayList<Integer> playerIds = new ArrayList<>();
        if (playerId <= 0) {
            return playerIds;
        }
        String sql =
            "SELECT i.other_player_id " +
            "FROM im_list i " +
            "LEFT JOIN users u ON u.user_id = i.other_player_id " +
            "WHERE i.player_id = ? " +
            "ORDER BY lower(COALESCE(u.username, '')), i.other_player_id";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    playerIds.add(rs.getInt(1));
                }
            }
        } catch (SQLException ignored) {
            // Keep empty fallback.
        }
        return playerIds;
    }

    public synchronized boolean getAreNewGuestbookEntriesAvailable(int playerId) {
        if (playerId <= 0) {
            return false;
        }
        String sql = "SELECT new_entries_available FROM guestbook_state WHERE player_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() && rs.getInt(1) != 0;
            }
        } catch (SQLException ignored) {
            return false;
        }
    }

    public synchronized void setAreNewGuestbookEntriesAvailable(int playerId, boolean available) throws IOException {
        if (playerId <= 0) {
            return;
        }
        String sql =
            "INSERT INTO guestbook_state (player_id, new_entries_available) VALUES (?, ?) " +
            "ON CONFLICT(player_id) DO UPDATE SET new_entries_available = excluded.new_entries_available";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, playerId);
            statement.setInt(2, available ? 1 : 0);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException("Failed to store guestbook availability state", e);
        }
    }

    private int countVisibleGuestbookEntries(int ownerPlayerId, int viewerPlayerId) {
        String sql =
            "SELECT COUNT(*) FROM guestbook_entries " +
            "WHERE owner_player_id = ? " +
            "AND (is_private = 0 OR owner_player_id = ? OR poster_player_id = ?)";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, ownerPlayerId);
            statement.setInt(2, viewerPlayerId);
            statement.setInt(3, viewerPlayerId);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        } catch (SQLException ignored) {
            return 0;
        }
    }

    private int countVisibleGuestbookHistory(int player1, int player2, int viewerPlayerId) {
        String sql =
            "SELECT COUNT(*) FROM guestbook_entries " +
            "WHERE ((owner_player_id = ? AND poster_player_id = ?) OR (owner_player_id = ? AND poster_player_id = ?)) " +
            "AND (is_private = 0 OR owner_player_id = ? OR poster_player_id = ?)";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, player1);
            statement.setInt(2, player2);
            statement.setInt(3, player2);
            statement.setInt(4, player1);
            statement.setInt(5, viewerPlayerId);
            statement.setInt(6, viewerPlayerId);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        } catch (SQLException ignored) {
            return 0;
        }
    }

    private GuestbookEntryRecord mapGuestbookEntry(ResultSet rs) throws SQLException {
        int postId = rs.getInt("id");
        int ownerPlayerId = rs.getInt("owner_player_id");
        int posterPlayerId = rs.getInt("poster_player_id");
        String posterName = safe(rs.getString("poster_name"));
        if (posterName.isEmpty()) {
            posterName = "Player " + posterPlayerId;
        }
        int posterLevel = rs.getInt("poster_level");
        if (rs.wasNull()) {
            posterLevel = 1;
        }
        return new GuestbookEntryRecord(
            postId,
            ownerPlayerId,
            posterPlayerId,
            posterName,
            posterLevel,
            safe(rs.getString("post_text")),
            rs.getLong("created_at"),
            rs.getInt("is_private") != 0,
            safe(rs.getString("post_type"))
        );
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
            "INSERT INTO avatar_state (player_id, avatar_data, mount_type, mount_name, pet_type, pet_name) " +
            "VALUES (?, ?, COALESCE(?, 0), COALESCE(?, ''), COALESCE(?, 0), COALESCE(?, '')) " +
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
            throw new IOException("Failed to store avatar state: " + e.getMessage(), e);
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

    private static byte[] defaultAvatarBlob() {
        return new byte[]{1};
    }

    private static ArrayList<Clothes> extractClothesFromAvatarBlob(int playerId, byte[] avatarData) {
        ArrayList<Clothes> result = new ArrayList<>();
        if (avatarData == null || avatarData.length < 28) {
            return result;
        }

        int bestOffset = -1;
        int bestCount = 0;
        for (int offset = 0; offset <= avatarData.length - 4; offset++) {
            int count = readIntBE(avatarData, offset);
            if (count <= 0 || count > 128) {
                continue;
            }
            int totalBytes = 4 + (count * 24);
            if (offset + totalBytes > avatarData.length) {
                continue;
            }
            boolean plausible = true;
            for (int i = 0; i < count; i++) {
                int entryOffset = offset + 4 + (i * 24);
                long clothId = readLongBE(avatarData, entryOffset);
                float hue = readFloatBE(avatarData, entryOffset + 8);
                float saturation = readFloatBE(avatarData, entryOffset + 12);
                float brightness = readFloatBE(avatarData, entryOffset + 16);
                float opacity = readFloatBE(avatarData, entryOffset + 20);
                if (clothId <= 0L ||
                    !Float.isFinite(hue) ||
                    !Float.isFinite(saturation) ||
                    !Float.isFinite(brightness) ||
                    !Float.isFinite(opacity) ||
                    Math.abs(hue) > 10f ||
                    Math.abs(saturation) > 10f ||
                    Math.abs(brightness) > 10f ||
                    Math.abs(opacity) > 10f) {
                    plausible = false;
                    break;
                }
            }
            if (plausible && count > bestCount) {
                bestCount = count;
                bestOffset = offset;
            }
        }

        if (bestOffset < 0 || bestCount <= 0) {
            return result;
        }

        for (int i = 0; i < bestCount; i++) {
            int entryOffset = bestOffset + 4 + (i * 24);
            long clothId = readLongBE(avatarData, entryOffset);
            float hue = readFloatBE(avatarData, entryOffset + 8);
            float saturation = readFloatBE(avatarData, entryOffset + 12);
            float brightness = readFloatBE(avatarData, entryOffset + 16);
            float opacity = readFloatBE(avatarData, entryOffset + 20);
            result.add(new Clothes(0, playerId, clothId, hue, saturation, brightness, opacity));
        }
        return result;
    }

    private static int readIntBE(byte[] data, int offset) {
        return ((data[offset] & 0xff) << 24)
            | ((data[offset + 1] & 0xff) << 16)
            | ((data[offset + 2] & 0xff) << 8)
            | (data[offset + 3] & 0xff);
    }

    private static long readLongBE(byte[] data, int offset) {
        return ((long) (data[offset] & 0xff) << 56)
            | ((long) (data[offset + 1] & 0xff) << 48)
            | ((long) (data[offset + 2] & 0xff) << 40)
            | ((long) (data[offset + 3] & 0xff) << 32)
            | ((long) (data[offset + 4] & 0xff) << 24)
            | ((long) (data[offset + 5] & 0xff) << 16)
            | ((long) (data[offset + 6] & 0xff) << 8)
            | ((long) (data[offset + 7] & 0xff));
    }

    private static float readFloatBE(byte[] data, int offset) {
        return Float.intBitsToFloat(readIntBE(data, offset));
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
