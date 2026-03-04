import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.slx.nord.jgnmessages.JGNMessages;
import com.slx.nord.jgnmessages.bytearraymessages.BlockPlayerMessage;
import com.slx.nord.jgnmessages.bytearraymessages.BuildingsChecksumResponseMessage;
import com.slx.nord.jgnmessages.bytearraymessages.GetBuildingsChecksumMessage;
import com.slx.nord.jgnmessages.bytearraymessages.LikedOnFacebookEventMessage;
import com.slx.nord.jgnmessages.bytearraymessages.LoggedOutByNewLoginMessage;
import com.slx.nord.jgnmessages.bytearraymessages.PlayerHasYouBlockedMessage;
import com.slx.nord.jgnmessages.bytearraymessages.RequestEventsResponseMessage;
import com.slx.nord.jgnmessages.bytearraymessages.SponsorpayNotificationMessage;
import com.slx.nord.jgnmessages.bytearraymessages.SuperRewardsNotificationMessage;
import com.slx.nord.jgnmessages.bytearraymessages.TeamBonusMessage;
import com.slx.nord.jgnmessages.bytearraymessages.UnblockPlayerMessage;
import com.slx.nord.jgnmessages.clientinbound.GenericByteArrayMessage;
import com.slx.nord.jgnmessages.clientinbound.notifications.EnterVillageWithAvatarNotificationMessage;
import com.slx.nord.jgnmessages.clientinbound.notifications.SendTextToPlayerNotificationMessage;
import com.slx.nord.jgnmessages.clientinbound.notifications.SendTextToVillageNotificationMessage;
import com.slx.nord.jgnmessages.clientinbound.responses.BuyCreditsResponseMessage;
import com.slx.nord.jgnmessages.clientinbound.responses.CreateAccountResponseMessage;
import com.slx.nord.jgnmessages.clientinbound.responses.IncomingUDPTestMessage;
import com.slx.nord.jgnmessages.clientinbound.responses.LoginResponseMessage;
import com.slx.nord.jgnmessages.clientinbound.responses.RequestBuildingsResponseMessage;
import com.slx.nord.jgnmessages.clientinbound.responses.RequestBlockedPlayersResponseMessage;
import com.slx.nord.jgnmessages.clientinbound.responses.RequestHeightMapResponseMessage;
import com.slx.nord.jgnmessages.clientinbound.responses.RequestOnlinePlayersResponseMessage;
import com.slx.nord.jgnmessages.clientinbound.responses.RequestPlayerInformationResponseMessage;
import com.slx.nord.jgnmessages.clientinbound.responses.RequestPlayerVillageIDsResponseMessage;
import com.slx.nord.jgnmessages.clientinbound.responses.RequestSLXCreditsResponseMessage;
import com.slx.nord.jgnmessages.clientinbound.responses.RequestVillageNameResponseMessage;
import com.slx.nord.jgnmessages.clientinbound.responses.RequestVillageVisitorsResponseMessage;
import com.slx.nord.jgnmessages.clientoutbound.AddEventMessage;
import com.slx.nord.jgnmessages.clientoutbound.BuyCreditsMessage;
import com.slx.nord.jgnmessages.clientoutbound.CompletedLevelMessage;
import com.slx.nord.jgnmessages.clientoutbound.CreateAccountMessage;
import com.slx.nord.jgnmessages.clientoutbound.ListenToVillageMessage;
import com.slx.nord.jgnmessages.clientoutbound.LoginMessage;
import com.slx.nord.jgnmessages.clientoutbound.RequestBuildingsMessage;
import com.slx.nord.jgnmessages.clientoutbound.RequestEventsMessage;
import com.slx.nord.jgnmessages.clientoutbound.RequestHeightMapMessage;
import com.slx.nord.jgnmessages.clientoutbound.RequestOnlinePlayersMessage;
import com.slx.nord.jgnmessages.clientoutbound.RequestPlayerInformationMessage;
import com.slx.nord.jgnmessages.clientoutbound.RequestPlayerVillageIDsMessage;
import com.slx.nord.jgnmessages.clientoutbound.RequestSLXCreditsMessage;
import com.slx.nord.jgnmessages.clientoutbound.RequestVillageNameMessage;
import com.slx.nord.jgnmessages.clientoutbound.RequestVillageVisitorsMessage;
import com.slx.nord.jgnmessages.clientoutbound.SendTextToPlayerMessage;
import com.slx.nord.jgnmessages.clientoutbound.SendTextToVillageMessage;
import com.slx.nord.jgnmessages.clientoutbound.SpendSLXCreditsMessage;
import com.slx.nord.jgnmessages.clientoutbound.StoreSpinMessage;
import com.slx.nord.jgnmessages.clientoutbound.StorePlayerInformationMessage;
import com.slx.nord.jgnmessages.clientoutbound.OutgoingUDPTestMessage;
import com.slx.nord.jgnpersistentobjectdetails.Building;
import com.slx.nord.jgnpersistentobjectdetails.Event;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.nio.charset.StandardCharsets;

public class LiveProtocolSmoke {
    private static final int DEFAULT_TCP_PORT = 41210;
    private static final int DEFAULT_UDP_PORT = 41211;
    private static final long DEFAULT_CONNECT_TIMEOUT_MS = 10_000L;
    private static final long DEFAULT_MESSAGE_TIMEOUT_MS = 10_000L;

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
        boolean useUdp = false;
    }

    public static void main(String[] args) throws Exception {
        Config config = parseArgs(args);
        run(config);
    }

    private static void run(Config config) throws Exception {
        long stamp = System.currentTimeMillis();
        String username = "smoke_" + stamp;
        String password = "smoke_pass_" + stamp;
        String cityMarker = "Smoke City " + stamp;
        String firstNameMarker = "SmokeFirst" + stamp;
        String lastNameMarker = "SmokeLast" + stamp;
        String profilePictureUrlMarker = "http://example.invalid/profile/" + stamp + ".jpg";
        short countryMarker = 46;
        byte birthDateMarker = 17;
        byte birthMonthMarker = 6;
        short birthYearMarker = 1999;
        boolean wantsEmailMarker = true;
        String villageMessage = "smoke message " + stamp;
        String directMessage = "direct smoke " + stamp;
        short progressionSpinLevel = 12;
        short progressionCompletedLevel = 13;
        int expectedReplacementLevel = progressionCompletedLevel + 1;
        short progressionWins = 7;
        boolean[] progressionLit = new boolean[] {true, false, true, true, false, true, false, true};
        short[] progressionResources = new short[] {11, 22, 33, 44, 55, 66, 77, 88};

        int playerId;
        int villageId;

        SmokeClient createClient = connect(config);
        try {
            if (config.useUdp) {
                createClient.client.sendUDP(new OutgoingUDPTestMessage());
                waitFor(
                    createClient.pump,
                    IncomingUDPTestMessage.class,
                    config.messageTimeoutMs,
                    "incoming UDP test response"
                );
            }

            CreateAccountMessage createAccount = new CreateAccountMessage(
                username,
                password,
                "Smoke",
                "Tester",
                "smoke@example.com",
                0,
                (byte) 1,
                (byte) 1,
                (short) 2000,
                (byte) 0,
                0,
                0L,
                ""
            );
            createClient.client.sendTCP(createAccount);

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

            createClient.client.sendTCP(new RequestVillageNameMessage(0, villageId));
            RequestVillageNameResponseMessage villageNameResponse = waitFor(
                createClient.pump,
                RequestVillageNameResponseMessage.class,
                config.messageTimeoutMs,
                "request village name response"
            );
            ensure(villageNameResponse.getVillageID() == villageId,
                "Village name response villageID mismatch");
            ensure(villageNameResponse.getVillageName() != null && !villageNameResponse.getVillageName().trim().isEmpty(),
                "Village name should not be empty");
        } finally {
            closeClient(createClient);
        }

        SmokeClient loginClient = connect(config);
        try {
            String mac = "SMOKE-MAC-" + stamp;
            loginClient.client.sendTCP(new LoginMessage(username, password, "SMOKE-1", mac));

            LoginResponseMessage loginResponse = waitFor(
                loginClient.pump,
                LoginResponseMessage.class,
                config.messageTimeoutMs,
                "login response"
            );
            ensure(loginResponse.getResponseCode() == 0,
                "Login responseCode expected 0, got " + loginResponse.getResponseCode());
            ensure(loginResponse.getUserID() == playerId,
                "Login userID mismatch");
            int sessionId = loginResponse.getSessionID();
            ensure(sessionId > 0, "Login sessionID should be >0");

            loginClient.client.sendTCP(new RequestHeightMapMessage(sessionId, villageId));
            RequestHeightMapResponseMessage heightMapResponse = waitFor(
                loginClient.pump,
                RequestHeightMapResponseMessage.class,
                config.messageTimeoutMs,
                "request heightmap response"
            );
            ensure(heightMapResponse.getVillageID() == villageId, "HeightMap villageID mismatch");
            ensure(heightMapResponse.getVillageHeightMap() != null && heightMapResponse.getVillageHeightMap().length > 0,
                "HeightMap payload should not be empty");

            sendChecksumRequest(loginClient, villageId);
            ensureNoGenericPayload(
                loginClient.pump,
                BuildingsChecksumResponseMessage.class,
                1000L,
                "unexpected checksum response before buildings sync (home village)"
            );
            requestBuildingsAndAssertChecksum(loginClient, sessionId, villageId, config.messageTimeoutMs, "home village");

            loginClient.client.sendTCP(new ListenToVillageMessage(sessionId, villageId));
            EnterVillageWithAvatarNotificationMessage enterVillage = waitFor(
                loginClient.pump,
                EnterVillageWithAvatarNotificationMessage.class,
                config.messageTimeoutMs,
                "enter village notification"
            );
            ensure(enterVillage.getVillageID() == villageId, "EnterVillage villageID mismatch");

            loginClient.client.sendTCP(new RequestVillageVisitorsMessage(sessionId, villageId));
            RequestVillageVisitorsResponseMessage visitorsResponse = waitFor(
                loginClient.pump,
                RequestVillageVisitorsResponseMessage.class,
                config.messageTimeoutMs,
                "request village visitors response"
            );
            ensure(visitorsResponse.getVillageID() == villageId, "VillageVisitors villageID mismatch");
            ensure(listContainsNumber(visitorsResponse.getUserIDs(), playerId),
                "Village visitors should include logged-in player");

            loginClient.client.sendTCP(new RequestOnlinePlayersMessage(sessionId));
            RequestOnlinePlayersResponseMessage onlineResponse = waitFor(
                loginClient.pump,
                RequestOnlinePlayersResponseMessage.class,
                config.messageTimeoutMs,
                "request online players response"
            );
            ensure(listContainsNumber(onlineResponse.getUserIDs(), playerId),
                "Online players should include logged-in player");

            loginClient.client.sendTCP(new RequestSLXCreditsMessage(sessionId));
            RequestSLXCreditsResponseMessage creditsBeforeSpend = waitFor(
                loginClient.pump,
                RequestSLXCreditsResponseMessage.class,
                config.messageTimeoutMs,
                "request credits response before spend"
            );
            int startingCredits = creditsBeforeSpend.getAmount();
            int spendAmount = 123;
            ensure(startingCredits >= spendAmount,
                "Expected starting credits >= " + spendAmount + ", got " + startingCredits);

            loginClient.client.sendTCP(new SpendSLXCreditsMessage(sessionId, spendAmount, "live_smoke_spend"));
            RequestSLXCreditsResponseMessage creditsAfterSpend = waitFor(
                loginClient.pump,
                RequestSLXCreditsResponseMessage.class,
                config.messageTimeoutMs,
                "credits response after spend"
            );
            ensure(creditsAfterSpend.getAmount() == startingCredits - spendAmount,
                "Credits after spend mismatch, expected " + (startingCredits - spendAmount) +
                    " got " + creditsAfterSpend.getAmount());

            short smokeEventType = 42;
            byte[] smokeEventData = ("live_smoke_event_" + stamp).getBytes(StandardCharsets.UTF_8);
            loginClient.client.sendTCP(new AddEventMessage(sessionId, 0L, smokeEventType, smokeEventData));
            loginClient.client.sendTCP(new RequestEventsMessage(sessionId, playerId, 20, 0));
            RequestEventsResponseMessage eventsPayload = waitForGenericPayload(
                loginClient.pump,
                RequestEventsResponseMessage.class,
                config.messageTimeoutMs,
                "request events generic response"
            );
            ensure(eventsPayload.getOffset() == 0, "RequestEvents offset mismatch");
            ArrayList events = eventsPayload.getEvents();
            ensure(events != null && !events.isEmpty(), "RequestEvents should return at least one event");
            Object firstEventObject = events.get(0);
            ensure(firstEventObject instanceof Event,
                "RequestEvents first payload item should be Event, got " +
                    (firstEventObject == null ? "null" : firstEventObject.getClass().getName()));
            Event firstEvent = (Event) firstEventObject;
            ensure(firstEvent.getPlayer1() == playerId,
                "RequestEvents first event player1 mismatch");
            ensure(firstEvent.getType() == smokeEventType,
                "RequestEvents first event type mismatch");
            ensure(Arrays.equals(smokeEventData, firstEvent.getData()),
                "RequestEvents first event data mismatch");

            int creditsAfterBuyCredits = creditsAfterSpend.getAmount();
            boolean strictBuyCreditsModeDetected = false;

            BuyCreditsResponseMessage sponsorpayCredits = requestBuyCredits(
                loginClient,
                sessionId,
                "100",
                "sponsorpay_live_smoke_" + stamp,
                config.messageTimeoutMs,
                "sponsorpay buy-credits response"
            );
            if (sponsorpayCredits.getBoughtCredits() == 0 &&
                sponsorpayCredits.getTotalAvailableCredits() == creditsAfterBuyCredits) {
                strictBuyCreditsModeDetected = true;
            } else {
                ensure(sponsorpayCredits.getBoughtCredits() == 100,
                    "Sponsorpay boughtCredits mismatch");
                ensure(sponsorpayCredits.getTotalAvailableCredits() == creditsAfterBuyCredits + 100,
                    "Sponsorpay totalAvailableCredits mismatch");
                SponsorpayNotificationMessage sponsorpayNotification = waitForGenericPayload(
                    loginClient.pump,
                    SponsorpayNotificationMessage.class,
                    config.messageTimeoutMs,
                    "sponsorpay generic notification"
                );
                ensure(sponsorpayNotification.getCreditsEarned() == 100,
                    "Sponsorpay notification creditsEarned mismatch");
                ensure(sponsorpayNotification.getNewCreditSum() == sponsorpayCredits.getTotalAvailableCredits(),
                    "Sponsorpay notification newCreditSum mismatch");
                creditsAfterBuyCredits = sponsorpayCredits.getTotalAvailableCredits();

                BuyCreditsResponseMessage superRewardsCredits = requestBuyCredits(
                    loginClient,
                    sessionId,
                    "200",
                    "superrewards_live_smoke_" + stamp,
                    config.messageTimeoutMs,
                    "superrewards buy-credits response"
                );
                ensure(superRewardsCredits.getBoughtCredits() == 200,
                    "SuperRewards boughtCredits mismatch");
                ensure(superRewardsCredits.getTotalAvailableCredits() == creditsAfterBuyCredits + 200,
                    "SuperRewards totalAvailableCredits mismatch");
                SuperRewardsNotificationMessage superRewardsNotification = waitForGenericPayload(
                    loginClient.pump,
                    SuperRewardsNotificationMessage.class,
                    config.messageTimeoutMs,
                    "superrewards generic notification"
                );
                ensure(superRewardsNotification.getCreditsEarned() == 200,
                    "SuperRewards notification creditsEarned mismatch");
                ensure(superRewardsNotification.getNewCreditSum() == superRewardsCredits.getTotalAvailableCredits(),
                    "SuperRewards notification newCreditSum mismatch");
                creditsAfterBuyCredits = superRewardsCredits.getTotalAvailableCredits();

                BuyCreditsResponseMessage facebookCredits = requestBuyCredits(
                    loginClient,
                    sessionId,
                    "300",
                    "facebook_live_smoke_" + stamp,
                    config.messageTimeoutMs,
                    "facebook buy-credits response"
                );
                ensure(facebookCredits.getBoughtCredits() == 300,
                    "Facebook boughtCredits mismatch");
                ensure(facebookCredits.getTotalAvailableCredits() == creditsAfterBuyCredits + 300,
                    "Facebook totalAvailableCredits mismatch");
                LikedOnFacebookEventMessage facebookNotification = waitForGenericPayload(
                    loginClient.pump,
                    LikedOnFacebookEventMessage.class,
                    config.messageTimeoutMs,
                    "facebook generic notification"
                );
                ensure(facebookNotification.getCoins() == 300,
                    "Facebook notification coins mismatch");
                creditsAfterBuyCredits = facebookCredits.getTotalAvailableCredits();

                String teamBonusLog = "teambonus:7:SmokeCaptain:2:9:444";
                BuyCreditsResponseMessage teamBonusCredits = requestBuyCredits(
                    loginClient,
                    sessionId,
                    "400",
                    teamBonusLog,
                    config.messageTimeoutMs,
                    "team bonus buy-credits response"
                );
                ensure(teamBonusCredits.getBoughtCredits() == 400,
                    "TeamBonus boughtCredits mismatch");
                ensure(teamBonusCredits.getTotalAvailableCredits() == creditsAfterBuyCredits + 400,
                    "TeamBonus totalAvailableCredits mismatch");
                TeamBonusMessage teamBonusNotification = waitForGenericPayload(
                    loginClient.pump,
                    TeamBonusMessage.class,
                    config.messageTimeoutMs,
                    "team bonus generic notification"
                );
                ensure(teamBonusNotification.getBonusId() == 7,
                    "TeamBonus bonusId mismatch");
                ensure("SmokeCaptain".equals(teamBonusNotification.getFromPlayer()),
                    "TeamBonus fromPlayer mismatch");
                ensure(teamBonusNotification.getOtherBonusReceivers() == 2,
                    "TeamBonus otherBonusReceivers mismatch");
                ensure(teamBonusNotification.getLevelAchieved() == 9,
                    "TeamBonus levelAchieved mismatch");
                ensure(teamBonusNotification.getData() == 444,
                    "TeamBonus data mismatch");
                creditsAfterBuyCredits = teamBonusCredits.getTotalAvailableCredits();
            }

            StorePlayerInformationMessage storeInfo = new StorePlayerInformationMessage();
            storeInfo.setSessionID(sessionId);
            storeInfo.setFirstName(firstNameMarker);
            storeInfo.setLastName(lastNameMarker);
            storeInfo.setBirthDate(birthDateMarker);
            storeInfo.setBirthMonth(birthMonthMarker);
            storeInfo.setBirthYear(birthYearMarker);
            storeInfo.setUrlToPresentationPicture(profilePictureUrlMarker);
            storeInfo.setCountry(countryMarker);
            storeInfo.setCity(cityMarker);
            storeInfo.setWantsEmail(Boolean.valueOf(wantsEmailMarker));
            loginClient.client.sendTCP(storeInfo);

            loginClient.client.sendTCP(new RequestPlayerInformationMessage(sessionId, playerId));
            RequestPlayerInformationResponseMessage playerInfo = waitFor(
                loginClient.pump,
                RequestPlayerInformationResponseMessage.class,
                config.messageTimeoutMs,
                "request player information response"
            );
            ensure(playerInfo.getPlayerID() == playerId, "PlayerInformation playerID mismatch");
            ensure(firstNameMarker.equals(playerInfo.getFirstName()),
                "PlayerInformation firstName mismatch");
            ensure(lastNameMarker.equals(playerInfo.getLastName()),
                "PlayerInformation lastName mismatch");
            ensure(cityMarker.equals(playerInfo.getPresentation()),
                "PlayerInformation presentation mismatch, expected \"" + cityMarker +
                    "\" got \"" + playerInfo.getPresentation() + "\"");
            ensure(cityMarker.equals(playerInfo.getCity()),
                "PlayerInformation city mismatch, expected \"" + cityMarker + "\" got \"" + playerInfo.getCity() + "\"");
            ensure(Short.toString(countryMarker).equals(playerInfo.getCountry()),
                "PlayerInformation country mismatch");
            ensure(profilePictureUrlMarker.equals(playerInfo.getUrlToPresentationPicture()),
                "PlayerInformation profile picture URL mismatch");
            ensure(playerInfo.getWantsEmail() == wantsEmailMarker,
                "PlayerInformation wantsEmail mismatch");

            loginClient.client.sendTCP(new SendTextToVillageMessage(sessionId, villageId, villageMessage));
            SendTextToVillageNotificationMessage villageChat = waitFor(
                loginClient.pump,
                SendTextToVillageNotificationMessage.class,
                config.messageTimeoutMs,
                "send text to village notification"
            );
            ensure(villageChat.getVillageID() == villageId, "Village chat villageID mismatch");
            ensure(playerId == villageChat.getFromPlayerID(), "Village chat fromPlayerID mismatch");
            ensure(villageMessage.equals(villageChat.getMessage()),
                "Village chat message mismatch");

            SmokeClient peerClient = connect(config);
            int peerPlayerId;
            int peerSessionId;
            try {
                long peerStamp = stamp + 1L;
                String peerUser = "smoke_peer_" + peerStamp;
                String peerPass = "smoke_peer_pass_" + peerStamp;
                peerClient.client.sendTCP(new CreateAccountMessage(
                    peerUser,
                    peerPass,
                    "Peer",
                    "Tester",
                    "peer@example.com",
                    0,
                    (byte) 1,
                    (byte) 1,
                    (short) 2000,
                    (byte) 0,
                    0,
                    0L,
                    ""
                ));
                CreateAccountResponseMessage peerCreateResponse = waitFor(
                    peerClient.pump,
                    CreateAccountResponseMessage.class,
                    config.messageTimeoutMs,
                    "peer create account response"
                );
                ensure(peerCreateResponse.getResponseCode() == 0,
                    "Peer create account responseCode expected 0, got " + peerCreateResponse.getResponseCode());
                peerPlayerId = peerCreateResponse.getPlayerID();
                ensure(peerPlayerId > 0, "Peer playerID should be >0");

                peerClient.client.sendTCP(new LoginMessage(peerUser, peerPass, "SMOKE-PEER-1", "SMOKE-PEER-MAC-" + stamp));
                LoginResponseMessage peerLoginResponse = waitFor(
                    peerClient.pump,
                    LoginResponseMessage.class,
                    config.messageTimeoutMs,
                    "peer login response"
                );
                ensure(peerLoginResponse.getResponseCode() == 0,
                    "Peer login responseCode expected 0, got " + peerLoginResponse.getResponseCode());
                peerSessionId = peerLoginResponse.getSessionID();
                ensure(peerSessionId > 0, "Peer sessionID should be >0");

                loginClient.client.sendTCP(new RequestPlayerVillageIDsMessage(sessionId, peerPlayerId));
                RequestPlayerVillageIDsResponseMessage peerVillageIdsResponse = waitFor(
                    loginClient.pump,
                    RequestPlayerVillageIDsResponseMessage.class,
                    config.messageTimeoutMs,
                    "peer request village ids response"
                );
                ArrayList peerVillageIds = peerVillageIdsResponse.getIDs();
                ensure(peerVillageIds != null && !peerVillageIds.isEmpty(),
                    "Peer village ID list should not be empty");
                int peerVillageId = ((Number) peerVillageIds.get(0)).intValue();
                ensure(peerVillageId > 0, "Peer village ID should be >0");

                sendChecksumRequest(loginClient, peerVillageId);
                ensureNoGenericPayload(
                    loginClient.pump,
                    BuildingsChecksumResponseMessage.class,
                    1000L,
                    "unexpected checksum response before buildings sync (peer village)"
                );
                requestBuildingsAndAssertChecksum(
                    loginClient,
                    sessionId,
                    peerVillageId,
                    config.messageTimeoutMs,
                    "peer village"
                );

                loginClient.client.sendTCP(new SendTextToPlayerMessage(sessionId, peerPlayerId, directMessage));
                SendTextToPlayerNotificationMessage directChat = waitFor(
                    peerClient.pump,
                    SendTextToPlayerNotificationMessage.class,
                    config.messageTimeoutMs,
                    "send text to player notification"
                );
                ensure(directChat.getFromPlayerID() == playerId,
                    "Direct message fromPlayerID mismatch");
                ensure(directMessage.equals(directChat.getMessage()),
                    "Direct message payload mismatch");

                loginClient.client.sendTCP(
                    GenericByteArrayMessage.createSerializedByteArrayMessage(
                        new BlockPlayerMessage(playerId, peerPlayerId)
                    )
                );
                RequestBlockedPlayersResponseMessage blockedSnapshot = waitForMatching(
                    loginClient.pump,
                    RequestBlockedPlayersResponseMessage.class,
                    config.messageTimeoutMs,
                    "blocked players snapshot after block",
                    response -> response.getPlayerID() == playerId &&
                        listContainsNumber(response.getUserIDs(), peerPlayerId)
                );
                ensure(listContainsNumber(blockedSnapshot.getUserIDs(), peerPlayerId),
                    "Blocked players snapshot should include peer after block");

                peerClient.client.sendTCP(new SendTextToPlayerMessage(peerSessionId, playerId, "peer blocked message " + stamp));
                PlayerHasYouBlockedMessage blockedByPlayer = waitForGenericPayload(
                    peerClient.pump,
                    PlayerHasYouBlockedMessage.class,
                    config.messageTimeoutMs,
                    "player blocked generic response"
                );
                ensure(blockedByPlayer.getUserId() == playerId,
                    "PlayerHasYouBlocked userId mismatch");
                ensureNoMessage(
                    loginClient.pump,
                    SendTextToPlayerNotificationMessage.class,
                    1000L,
                    "unexpected direct-message delivery while sender is blocked"
                );

                loginClient.client.sendTCP(
                    GenericByteArrayMessage.createSerializedByteArrayMessage(
                        new UnblockPlayerMessage(playerId, peerPlayerId)
                    )
                );
                RequestBlockedPlayersResponseMessage unblockedSnapshot = waitForMatching(
                    loginClient.pump,
                    RequestBlockedPlayersResponseMessage.class,
                    config.messageTimeoutMs,
                    "blocked players snapshot after unblock",
                    response -> response.getPlayerID() == playerId &&
                        !listContainsNumber(response.getUserIDs(), peerPlayerId)
                );
                ensure(!listContainsNumber(unblockedSnapshot.getUserIDs(), peerPlayerId),
                    "Blocked players snapshot should not include peer after unblock");

                String peerToOwnerMessage = "peer unblocked message " + stamp;
                peerClient.client.sendTCP(new SendTextToPlayerMessage(peerSessionId, playerId, peerToOwnerMessage));
                SendTextToPlayerNotificationMessage unblockedDirectChat = waitForMatching(
                    loginClient.pump,
                    SendTextToPlayerNotificationMessage.class,
                    config.messageTimeoutMs,
                    "direct message notification after unblock",
                    notification -> notification.getFromPlayerID() == peerPlayerId
                );
                ensure(peerToOwnerMessage.equals(unblockedDirectChat.getMessage()),
                    "Direct message payload mismatch after unblock");

                StoreSpinMessage storeSpinSnapshot = new StoreSpinMessage(
                    sessionId,
                    progressionSpinLevel,
                    progressionWins,
                    true,
                    progressionResources[0],
                    progressionResources[1],
                    progressionResources[2],
                    progressionResources[3],
                    progressionResources[4],
                    progressionResources[5],
                    progressionResources[6],
                    progressionResources[7],
                    progressionLit[0],
                    progressionLit[1],
                    progressionLit[2],
                    progressionLit[3],
                    progressionLit[4],
                    progressionLit[5],
                    progressionLit[6],
                    progressionLit[7]
                );
                loginClient.client.sendTCP(storeSpinSnapshot);

                loginClient.client.sendTCP(new CompletedLevelMessage(sessionId, playerId, progressionCompletedLevel));
                RequestSLXCreditsResponseMessage completedLevelCredits = waitFor(
                    loginClient.pump,
                    RequestSLXCreditsResponseMessage.class,
                    config.messageTimeoutMs,
                    "completed level credits response"
                );
                ensure(completedLevelCredits.getAmount() >= 0,
                    "Completed level credits response should be non-negative");

                SmokeClient replacementClient = connect(config);
                try {
                    replacementClient.client.sendTCP(new LoginMessage(username, password, "SMOKE-REPLACEMENT-1", "SMOKE-REPLACEMENT-MAC-" + stamp));
                    LoginResponseMessage replacementLoginResponse = waitFor(
                        replacementClient.pump,
                        LoginResponseMessage.class,
                        config.messageTimeoutMs,
                        "replacement login response"
                    );
                    ensure(replacementLoginResponse.getResponseCode() == 0,
                        "Replacement login responseCode expected 0, got " + replacementLoginResponse.getResponseCode());
                    ensure(replacementLoginResponse.getUserID() == playerId,
                        "Replacement login userID mismatch");
                    int replacementSessionId = replacementLoginResponse.getSessionID();
                    ensure(replacementSessionId > 0, "Replacement sessionID should be >0");
                    ensure(replacementLoginResponse.getLevel() == expectedReplacementLevel,
                        "Replacement login level mismatch expected " + expectedReplacementLevel +
                            " got " + replacementLoginResponse.getLevel());
                    ensure(replacementLoginResponse.getSpins() == progressionWins,
                        "Replacement login spins mismatch expected " + progressionWins +
                            " got " + replacementLoginResponse.getSpins());
                    assertWheelResourcePayload(
                        replacementLoginResponse.getData(),
                        progressionLit,
                        progressionResources
                    );

                    LoggedOutByNewLoginMessage loggedOutByReplacement = waitForGenericPayload(
                        loginClient.pump,
                        LoggedOutByNewLoginMessage.class,
                        config.messageTimeoutMs,
                        "logged out by new login generic notification"
                    );
                    ensure(loggedOutByReplacement.isTerminate(),
                        "LoggedOutByNewLogin terminate flag should be true");

                    sendChecksumRequest(replacementClient, villageId);
                    ensureNoGenericPayload(
                        replacementClient.pump,
                        BuildingsChecksumResponseMessage.class,
                        1000L,
                        "unexpected checksum response before buildings sync (replacement session)"
                    );
                    requestBuildingsAndAssertChecksum(
                        replacementClient,
                        replacementSessionId,
                        villageId,
                        config.messageTimeoutMs,
                        "replacement home village"
                    );
                } finally {
                    closeClient(replacementClient);
                }
            } finally {
                closeClient(peerClient);
            }

            String buyCreditsMode = strictBuyCreditsModeDetected ? "provider_callback_only" : "code_map_enabled";
            System.out.println(
                "LIVE_SMOKE_PASS user=" + username +
                    " playerId=" + playerId +
                    " villageId=" + villageId +
                    " buyCreditsMode=" + buyCreditsMode
            );
        } finally {
            closeClient(loginClient);
        }
    }

    private static BuyCreditsResponseMessage requestBuyCredits(SmokeClient client,
                                                               int sessionId,
                                                               String code,
                                                               String logMessage,
                                                               long timeoutMs,
                                                               String label) throws InterruptedException {
        client.client.sendTCP(new BuyCreditsMessage(sessionId, code, logMessage));
        BuyCreditsResponseMessage response = waitFor(
            client.pump,
            BuyCreditsResponseMessage.class,
            timeoutMs,
            label
        );
        ensure(response.getBoughtCredits() >= 0,
            "BuyCredits response indicates failure for label=" + label +
                " boughtCredits=" + response.getBoughtCredits());
        return response;
    }

    private static void sendChecksumRequest(SmokeClient client, int villageId) {
        try {
            client.client.sendTCP(
                GenericByteArrayMessage.createSerializedByteArrayMessage(
                    new GetBuildingsChecksumMessage(villageId)
                )
            );
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize GetBuildingsChecksumMessage", e);
        }
    }

    private static void requestBuildingsAndAssertChecksum(SmokeClient client,
                                                          int sessionId,
                                                          int villageId,
                                                          long timeoutMs,
                                                          String labelPrefix) throws InterruptedException {
        client.client.sendTCP(new RequestBuildingsMessage(sessionId, villageId));
        RequestBuildingsResponseMessage buildingsResponse = waitForMatching(
            client.pump,
            RequestBuildingsResponseMessage.class,
            timeoutMs,
            labelPrefix + " request buildings response",
            response -> response.getVillageID() == villageId
        );
        ensure(buildingsResponse.getBuildings() != null,
            labelPrefix + " buildings list should not be null");
        long expectedBuildingsChecksum = calculateBuildingsChecksum(buildingsResponse.getBuildings());

        sendChecksumRequest(client, villageId);
        BuildingsChecksumResponseMessage checksumResponse = waitForGenericPayloadMatching(
            client.pump,
            BuildingsChecksumResponseMessage.class,
            timeoutMs,
            labelPrefix + " buildings checksum response",
            response -> response.getVillageID() == villageId
        );
        ensure(checksumResponse.getChecksum() == expectedBuildingsChecksum,
            labelPrefix + " buildings checksum mismatch expected=" + expectedBuildingsChecksum +
                " actual=" + checksumResponse.getChecksum());
    }

    private static long calculateBuildingsChecksum(ArrayList buildings) {
        long checksum = Long.MIN_VALUE;
        if (buildings == null) {
            return checksum;
        }
        for (Object value : buildings) {
            if (!(value instanceof Building)) {
                continue;
            }
            Building building = (Building) value;
            checksum += (long) building.getBuildingID();
            checksum += building.isConsumed() ? 1L : 0L;
            checksum += (long) ((byte) building.getTileX());
            checksum += (long) ((byte) building.getTileZ());
        }
        return checksum;
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
                Thread.sleep(300L * attempt);
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
                                 String label) throws InterruptedException {
        return waitForMatching(pump, messageType, timeoutMs, label, value -> true);
    }

    private static <T> T waitForMatching(MessagePump pump,
                                         Class<T> messageType,
                                         long timeoutMs,
                                         String label,
                                         Predicate<T> predicate) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            long remaining = Math.max(1L, deadline - System.currentTimeMillis());
            Object msg = pump.poll(Math.min(remaining, 250L));
            if (msg == null) {
                continue;
            }
            if (messageType.isInstance(msg)) {
                T cast = messageType.cast(msg);
                if (predicate == null || predicate.test(cast)) {
                    return cast;
                }
            }
        }
        throw new IllegalStateException("Timed out waiting for " + label + " (" + messageType.getSimpleName() + ")");
    }

    private static <T> T waitForGenericPayload(MessagePump pump,
                                               Class<T> payloadType,
                                               long timeoutMs,
                                               String label) throws InterruptedException {
        return waitForGenericPayloadMatching(pump, payloadType, timeoutMs, label, value -> true);
    }

    private static <T> T waitForGenericPayloadMatching(MessagePump pump,
                                                       Class<T> payloadType,
                                                       long timeoutMs,
                                                       String label,
                                                       Predicate<T> predicate) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            long remaining = Math.max(1L, deadline - System.currentTimeMillis());
            Object msg = pump.poll(Math.min(remaining, 250L));
            if (!(msg instanceof GenericByteArrayMessage)) {
                continue;
            }
            Object payload = deserializeGenericPayload((GenericByteArrayMessage) msg);
            if (payloadType.isInstance(payload)) {
                T cast = payloadType.cast(payload);
                if (predicate == null || predicate.test(cast)) {
                    return cast;
                }
            }
        }
        throw new IllegalStateException("Timed out waiting for " + label + " (" + payloadType.getSimpleName() + ")");
    }

    private static Object deserializeGenericPayload(GenericByteArrayMessage message) {
        if (message == null || message.getType() != 0) {
            return null;
        }
        byte[] data = message.getData();
        if (data == null || data.length == 0) {
            return null;
        }
        try (ByteArrayInputStream in = new ByteArrayInputStream(data);
             ObjectInputStream objectInput = new ObjectInputStream(in)) {
            return objectInput.readObject();
        } catch (Exception ignored) {
            return null;
        }
    }

    private static void assertWheelResourcePayload(byte[] payload,
                                                   boolean[] expectedLit,
                                                   short[] expectedResources) {
        ensure(expectedLit != null && expectedLit.length == 8,
            "Expected lit array must contain 8 entries");
        ensure(expectedResources != null && expectedResources.length == 8,
            "Expected resources array must contain 8 entries");
        ensure(payload != null && payload.length > 0,
            "Expected non-empty wheel resource payload");
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            int version = in.readUnsignedShort();
            ensure(version == 1, "Wheel resource payload version mismatch expected 1 got " + version);
            for (int i = 0; i < expectedLit.length; i++) {
                boolean actual = in.readBoolean();
                ensure(actual == expectedLit[i],
                    "Wheel resource lit" + (i + 1) + " mismatch expected " + expectedLit[i] +
                        " got " + actual);
            }
            for (int i = 0; i < expectedResources.length; i++) {
                short actual = in.readShort();
                ensure(actual == expectedResources[i],
                    "Wheel resource res" + (i + 1) + " mismatch expected " + expectedResources[i] +
                        " got " + actual);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to decode wheel resource payload", e);
        }
    }

    private static void ensureNoMessage(MessagePump pump,
                                        Class<?> forbiddenType,
                                        long timeoutMs,
                                        String label) throws InterruptedException {
        long deadline = System.currentTimeMillis() + Math.max(1L, timeoutMs);
        while (System.currentTimeMillis() < deadline) {
            long remaining = Math.max(1L, deadline - System.currentTimeMillis());
            Object msg = pump.poll(Math.min(remaining, 250L));
            if (msg == null) {
                continue;
            }
            if (forbiddenType.isInstance(msg)) {
                throw new IllegalStateException(label + " (" + forbiddenType.getSimpleName() + ")");
            }
        }
    }

    private static void ensureNoGenericPayload(MessagePump pump,
                                               Class<?> forbiddenPayloadType,
                                               long timeoutMs,
                                               String label) throws InterruptedException {
        long deadline = System.currentTimeMillis() + Math.max(1L, timeoutMs);
        while (System.currentTimeMillis() < deadline) {
            long remaining = Math.max(1L, deadline - System.currentTimeMillis());
            Object msg = pump.poll(Math.min(remaining, 250L));
            if (!(msg instanceof GenericByteArrayMessage)) {
                continue;
            }
            Object payload = deserializeGenericPayload((GenericByteArrayMessage) msg);
            if (forbiddenPayloadType.isInstance(payload)) {
                throw new IllegalStateException(label + " (" + forbiddenPayloadType.getSimpleName() + ")");
            }
        }
    }

    private static boolean listContainsNumber(ArrayList values, int expected) {
        if (values == null) {
            return false;
        }
        for (Object value : values) {
            if (value instanceof Number && ((Number) value).intValue() == expected) {
                return true;
            }
        }
        return false;
    }

    private static void ensure(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static Config parseArgs(String[] args) {
        Config config = new Config();
        for (String arg : args) {
            if (arg == null || arg.trim().isEmpty()) {
                continue;
            }
            String trimmed = arg.trim();
            if ("--use-udp".equals(trimmed)) {
                config.useUdp = true;
                continue;
            }
            int idx = trimmed.indexOf('=');
            if (idx <= 2 || !trimmed.startsWith("--")) {
                throw new IllegalArgumentException("Invalid argument: " + trimmed);
            }
            String key = trimmed.substring(2, idx).toLowerCase(Locale.ROOT);
            String value = trimmed.substring(idx + 1).trim();
            if ("host".equals(key)) {
                config.host = value;
            } else if ("tcp-port".equals(key)) {
                config.tcpPort = Integer.parseInt(value);
            } else if ("udp-port".equals(key)) {
                config.udpPort = Integer.parseInt(value);
            } else if ("connect-timeout-ms".equals(key)) {
                config.connectTimeoutMs = Long.parseLong(value);
            } else if ("message-timeout-ms".equals(key)) {
                config.messageTimeoutMs = Long.parseLong(value);
            } else {
                throw new IllegalArgumentException("Unknown argument: " + trimmed);
            }
        }
        return config;
    }
}
