/*
 * AmstaMatchaXXX — Canal-side discovery and optional messaging for adults.
 * Venue listings, time slots, bookings and private notes; single-file Java engine.
 */

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

// -----------------------------------------------------------------------------
// EXCEPTIONS (AMM = AmstaMatcha)
// -----------------------------------------------------------------------------

final class AMMException extends RuntimeException {
    private final String code;

    AMMException(String code, String message) {
        super(message);
        this.code = code;
    }

    String getCode() {
        return code;
    }
}

// -----------------------------------------------------------------------------
// ERROR CODES
// -----------------------------------------------------------------------------

final class AMMErrorCodes {
    static final String AMM_ZERO_ADDRESS = "AMM_ZERO_ADDRESS";
    static final String AMM_ZERO_AMOUNT = "AMM_ZERO_AMOUNT";
    static final String AMM_VENUE_NOT_FOUND = "AMM_VENUE_NOT_FOUND";
    static final String AMM_SLOT_NOT_FOUND = "AMM_SLOT_NOT_FOUND";
    static final String AMM_BOOKING_NOT_FOUND = "AMM_BOOKING_NOT_FOUND";
    static final String AMM_MESSAGE_NOT_FOUND = "AMM_MESSAGE_NOT_FOUND";
    static final String AMM_NOT_CURATOR = "AMM_NOT_CURATOR";
    static final String AMM_NOT_GUIDE = "AMM_NOT_GUIDE";
    static final String AMM_SLOT_UNAVAILABLE = "AMM_SLOT_UNAVAILABLE";
    static final String AMM_BOOKING_EXISTS = "AMM_BOOKING_EXISTS";
    static final String AMM_MAX_VENUES = "AMM_MAX_VENUES";
    static final String AMM_MAX_SLOTS = "AMM_MAX_SLOTS";
    static final String AMM_MAX_BOOKINGS = "AMM_MAX_BOOKINGS";
    static final String AMM_MAX_MESSAGES = "AMM_MAX_MESSAGES";
    static final String AMM_NAMESPACE_FROZEN = "AMM_NAMESPACE_FROZEN";
    static final String AMM_REENTRANT = "AMM_REENTRANT";
    static final String AMM_INVALID_FEE = "AMM_INVALID_FEE";
    static final String AMM_INVALID_DURATION = "AMM_INVALID_DURATION";
    static final String AMM_MESSAGE_DISABLED = "AMM_MESSAGE_DISABLED";

    static String describe(String code) {
        if (code == null) return "Unknown";
        switch (code) {
            case AMM_ZERO_ADDRESS: return "Address invalid";
            case AMM_ZERO_AMOUNT: return "Amount must be positive";
            case AMM_VENUE_NOT_FOUND: return "Venue not found";
            case AMM_SLOT_NOT_FOUND: return "Slot not found";
            case AMM_BOOKING_NOT_FOUND: return "Booking not found";
            case AMM_MESSAGE_NOT_FOUND: return "Message not found";
            case AMM_NOT_CURATOR: return "Not curator";
            case AMM_NOT_GUIDE: return "Not guide";
            case AMM_SLOT_UNAVAILABLE: return "Slot unavailable";
            case AMM_BOOKING_EXISTS: return "Booking already exists";
            case AMM_MAX_VENUES: return "Max venues reached";
            case AMM_MAX_SLOTS: return "Max slots reached";
            case AMM_MAX_BOOKINGS: return "Max bookings reached";
            case AMM_MAX_MESSAGES: return "Max messages reached";
            case AMM_NAMESPACE_FROZEN: return "Namespace frozen";
            case AMM_REENTRANT: return "Reentrant call";
            case AMM_INVALID_FEE: return "Fee out of range";
            case AMM_INVALID_DURATION: return "Invalid duration";
            case AMM_MESSAGE_DISABLED: return "Messaging disabled";
            default: return "Unknown: " + code;
        }
    }
}

// -----------------------------------------------------------------------------
// CONSTANTS
// -----------------------------------------------------------------------------

final class AMMConstants {
    static final int AMM_MAX_VENUES = 384;
    static final int AMM_MAX_SLOTS_PER_VENUE = 96;
    static final int AMM_MAX_BOOKINGS_PER_USER = 24;
    static final int AMM_MAX_MESSAGES_PER_THREAD = 512;
    static final int AMM_MAX_THREADS = 2048;
    static final int AMM_FEE_BPS_CAP = 500;
    static final BigDecimal AMM_ONE = new BigDecimal("1", new MathContext(18, RoundingMode.HALF_UP));
    static final MathContext MC = new MathContext(18, RoundingMode.HALF_UP);
    static final String AMM_NAMESPACE = "amsta-matcha-xxx.v1";
}

// -----------------------------------------------------------------------------
// ENUMS
// -----------------------------------------------------------------------------

enum AMMVenueType {
    CANAL_HOUSE,
    LOUNGE,
    PRIVATE_STUDIO,
    EXPERIENCE_ROOM
}

enum AMMSlotStatus {
    OPEN,
    BOOKED,
    CANCELLED
}

enum AMMBookingStatus {
    PENDING,
    CONFIRMED,
    COMPLETED,
    CANCELLED
}

enum AMMMessageStatus {
    SENT,
    DELIVERED,
    READ
}

// -----------------------------------------------------------------------------
// EVENTS (log-style records)
// -----------------------------------------------------------------------------

final class AMMVenueAdded {
    private final String venueId;
    private final String curator;
    private final long atEpoch;

    AMMVenueAdded(String venueId, String curator, long atEpoch) {
        this.venueId = venueId;
        this.curator = curator;
        this.atEpoch = atEpoch;
    }

    String getVenueId() { return venueId; }
    String getCurator() { return curator; }
    long getAtEpoch() { return atEpoch; }
}

final class AMMSlotListed {
    private final String slotId;
    private final String venueId;
    private final long startEpoch;
    private final long endEpoch;
    private final long atEpoch;

    AMMSlotListed(String slotId, String venueId, long startEpoch, long endEpoch, long atEpoch) {
        this.slotId = slotId;
        this.venueId = venueId;
        this.startEpoch = startEpoch;
        this.endEpoch = endEpoch;
        this.atEpoch = atEpoch;
    }

    String getSlotId() { return slotId; }
    String getVenueId() { return venueId; }
    long getStartEpoch() { return startEpoch; }
    long getEndEpoch() { return endEpoch; }
    long getAtEpoch() { return atEpoch; }
}

final class AMMTourBooked {
    private final String bookingId;
    private final String slotId;
    private final String guest;
    private final String guide;
    private final BigDecimal amountWei;
    private final long atEpoch;

    AMMTourBooked(String bookingId, String slotId, String guest, String guide, BigDecimal amountWei, long atEpoch) {
        this.bookingId = bookingId;
        this.slotId = slotId;
        this.guest = guest;
        this.guide = guide;
        this.amountWei = amountWei;
        this.atEpoch = atEpoch;
    }

    String getBookingId() { return bookingId; }
    String getSlotId() { return slotId; }
    String getGuest() { return guest; }
    String getGuide() { return guide; }
    BigDecimal getAmountWei() { return amountWei; }
    long getAtEpoch() { return atEpoch; }
}

final class AMMMessageSent {
    private final String threadId;
    private final String fromAddr;
    private final String toAddr;
    private final String messageHash;
    private final long atEpoch;

    AMMMessageSent(String threadId, String fromAddr, String toAddr, String messageHash, long atEpoch) {
        this.threadId = threadId;
        this.fromAddr = fromAddr;
        this.toAddr = toAddr;
        this.messageHash = messageHash;
        this.atEpoch = atEpoch;
    }

    String getThreadId() { return threadId; }
    String getFromAddr() { return fromAddr; }
    String getToAddr() { return toAddr; }
    String getMessageHash() { return messageHash; }
    long getAtEpoch() { return atEpoch; }
}

// -----------------------------------------------------------------------------
// CONFIG
// -----------------------------------------------------------------------------

final class AMMConfig {
    private final BigDecimal feeBps;
    private final boolean messagingEnabled;
    private final boolean namespaceFrozen;

    AMMConfig(BigDecimal feeBps, boolean messagingEnabled, boolean namespaceFrozen) {
        this.feeBps = feeBps;
        this.messagingEnabled = messagingEnabled;
        this.namespaceFrozen = namespaceFrozen;
    }

    BigDecimal getFeeBps() { return feeBps; }
    boolean isMessagingEnabled() { return messagingEnabled; }
    boolean isNamespaceFrozen() { return namespaceFrozen; }
}

// -----------------------------------------------------------------------------
// ENTITIES
// -----------------------------------------------------------------------------

final class AMMVenue {
    private final String venueId;
    private final String name;
    private final AMMVenueType venueType;
    private final String curator;
    private final long createdAtEpoch;

    AMMVenue(String venueId, String name, AMMVenueType venueType, String curator, long createdAtEpoch) {
        this.venueId = venueId;
        this.name = name;
        this.venueType = venueType;
        this.curator = curator;
        this.createdAtEpoch = createdAtEpoch;
    }

    String getVenueId() { return venueId; }
    String getName() { return name; }
    AMMVenueType getVenueType() { return venueType; }
    String getCurator() { return curator; }
    long getCreatedAtEpoch() { return createdAtEpoch; }
}

final class AMMSlot {
    private final String slotId;
    private final String venueId;
    private final long startEpoch;
    private final long endEpoch;
    private AMMSlotStatus status;
    private final String guideAddr;

    AMMSlot(String slotId, String venueId, long startEpoch, long endEpoch, String guideAddr) {
        this.slotId = slotId;
        this.venueId = venueId;
        this.startEpoch = startEpoch;
        this.endEpoch = endEpoch;
        this.guideAddr = guideAddr;
        this.status = AMMSlotStatus.OPEN;
    }

    String getSlotId() { return slotId; }
    String getVenueId() { return venueId; }
    long getStartEpoch() { return startEpoch; }
    long getEndEpoch() { return endEpoch; }
    AMMSlotStatus getStatus() { return status; }
    void setStatus(AMMSlotStatus status) { this.status = status; }
    String getGuideAddr() { return guideAddr; }
}

final class AMMBooking {
    private final String bookingId;
    private final String slotId;
    private final String guest;
    private final String guide;
    private final BigDecimal amountWei;
    private AMMBookingStatus status;
    private final long createdAtEpoch;

    AMMBooking(String bookingId, String slotId, String guest, String guide, BigDecimal amountWei, long createdAtEpoch) {
        this.bookingId = bookingId;
        this.slotId = slotId;
        this.guest = guest;
        this.guide = guide;
        this.amountWei = amountWei;
        this.createdAtEpoch = createdAtEpoch;
        this.status = AMMBookingStatus.CONFIRMED;
    }

    String getBookingId() { return bookingId; }
    String getSlotId() { return slotId; }
    String getGuest() { return guest; }
    String getGuide() { return guide; }
    BigDecimal getAmountWei() { return amountWei; }
    AMMBookingStatus getStatus() { return status; }
    void setStatus(AMMBookingStatus status) { this.status = status; }
    long getCreatedAtEpoch() { return createdAtEpoch; }
}

final class AMMMessage {
    private final String messageId;
    private final String threadId;
    private final String fromAddr;
    private final String toAddr;
    private final String contentHash;
    private AMMMessageStatus status;
    private final long sentAtEpoch;

    AMMMessage(String messageId, String threadId, String fromAddr, String toAddr, String contentHash, long sentAtEpoch) {
        this.messageId = messageId;
        this.threadId = threadId;
        this.fromAddr = fromAddr;
        this.toAddr = toAddr;
        this.contentHash = contentHash;
        this.sentAtEpoch = sentAtEpoch;
        this.status = AMMMessageStatus.SENT;
    }

    String getMessageId() { return messageId; }
    String getThreadId() { return threadId; }
    String getFromAddr() { return fromAddr; }
    String getToAddr() { return toAddr; }
    String getContentHash() { return contentHash; }
    AMMMessageStatus getStatus() { return status; }
    void setStatus(AMMMessageStatus status) { this.status = status; }
    long getSentAtEpoch() { return sentAtEpoch; }
}

// -----------------------------------------------------------------------------
// AMSTAMATCHA XXX — MAIN ENGINE
// -----------------------------------------------------------------------------

public final class AmstaMatchaXXX {

    // Immutable addresses (EIP-55, 40 hex)
    private final String curator;
    private final String treasury;
    private final String messageRelay;
    private final String feeCollector;
    private final String backupCurator;
    private final long deployEpoch;

    private final Map<String, AMMVenue> venuesById = new ConcurrentHashMap<>();
    private final Map<String, AMMSlot> slotsById = new ConcurrentHashMap<>();
    private final Map<String, AMMBooking> bookingsById = new ConcurrentHashMap<>();
    private final Map<String, AMMMessage> messagesById = new ConcurrentHashMap<>();
    private final Map<String, List<String>> slotIdsByVenue = new ConcurrentHashMap<>();
    private final Map<String, List<String>> bookingIdsByGuest = new ConcurrentHashMap<>();
    private final Map<String, List<String>> messageIdsByThread = new ConcurrentHashMap<>();
    private final Map<String, String> threadByParticipantPair = new ConcurrentHashMap<>();

    private AMMConfig config;
    private int reentrancyLock = 0;
    private BigDecimal totalFeesCollected = BigDecimal.ZERO;

    private static final int AMM_MAX_EVENTS = 256;
    private final List<AMMVenueAdded> venueAddedEvents = new ArrayList<>();
    private final List<AMMSlotListed> slotListedEvents = new ArrayList<>();
    private final List<AMMTourBooked> tourBookedEvents = new ArrayList<>();
    private final List<AMMMessageSent> messageSentEvents = new ArrayList<>();

    public AmstaMatchaXXX() {
        this.curator = "0xCd2A3d9F1E7b4C6a8D0e2F5A7c9B1d3E6f8A0C2";
        this.treasury = "0xBe1F4a7C0d3E6b9F2a5C8d1E4f7A0b3D6e9F2a5";
        this.messageRelay = "0x9F2e5A8c1D4f7B0a3E6d9C2f5A8b1E4d7C0a3F6";
        this.feeCollector = "0x8E1d4F7a0C3e6B9f2A5c8D1e4F7a0B3d6E9f2A5";
        this.backupCurator = "0x7D0c3F6a9E2b5D8f1A4c7E0b3F6a9D2e5C8f1B4";
        this.deployEpoch = Instant.now().getEpochSecond();
        this.config = new AMMConfig(new BigDecimal("45", AMMConstants.MC), true, false);
    }

    public String getCurator() { return curator; }
    public String getTreasury() { return treasury; }
    public String getMessageRelay() { return messageRelay; }
    public String getFeeCollector() { return feeCollector; }
    public String getBackupCurator() { return backupCurator; }
    public long getDeployEpoch() { return deployEpoch; }
    public AMMConfig getConfig() { return config; }

    private void requireCurator(String sender) {
        if (sender == null || (!sender.equals(curator) && !sender.equals(backupCurator)))
            throw new AMMException(AMMErrorCodes.AMM_NOT_CURATOR, "Not curator");
    }

    private void requireNotFrozen() {
        if (config.isNamespaceFrozen())
            throw new AMMException(AMMErrorCodes.AMM_NAMESPACE_FROZEN, "Frozen");
    }

    private void requireNotReentrant() {
        if (reentrancyLock != 0)
            throw new AMMException(AMMErrorCodes.AMM_REENTRANT, "Reentrant");
    }

    private void requireMessagingEnabled() {
        if (!config.isMessagingEnabled())
            throw new AMMException(AMMErrorCodes.AMM_MESSAGE_DISABLED, "Messaging disabled");
    }

    // -------------------------------------------------------------------------
    // VENUES
    // -------------------------------------------------------------------------

    public synchronized String addVenue(String sender, String venueId, String name, AMMVenueType venueType) {
        requireCurator(sender);
        requireNotReentrant();
        requireNotFrozen();
        if (venueId == null || venueId.isEmpty()) throw new AMMException(AMMErrorCodes.AMM_ZERO_ADDRESS, "Venue id");
        if (venuesById.size() >= AMMConstants.AMM_MAX_VENUES)
            throw new AMMException(AMMErrorCodes.AMM_MAX_VENUES, "Max venues");
        if (venuesById.containsKey(venueId))
            throw new AMMException(AMMErrorCodes.AMM_BOOKING_EXISTS, "Venue exists");

        reentrancyLock = 1;
        try {
            long now = Instant.now().getEpochSecond();
            AMMVenue v = new AMMVenue(venueId, name != null ? name : "Unnamed", venueType, sender, now);
            venuesById.put(venueId, v);
            slotIdsByVenue.put(venueId, new ArrayList<>());
            if (venueAddedEvents.size() < AMM_MAX_EVENTS)
                venueAddedEvents.add(new AMMVenueAdded(venueId, sender, now));
            return venueId;
        } finally {
            reentrancyLock = 0;
        }
    }

    public AMMVenue getVenue(String venueId) {
        return venuesById.get(venueId);
    }

    public List<AMMVenue> listVenues() {
        return new ArrayList<>(venuesById.values());
    }

    public int getVenueCount() {
        return venuesById.size();
    }

    // -------------------------------------------------------------------------
    // SLOTS
    // -------------------------------------------------------------------------

    public synchronized String listSlot(String sender, String slotId, String venueId, long startEpoch, long endEpoch) {
        requireCurator(sender);
        requireNotReentrant();
        requireNotFrozen();
        if (slotId == null || venueId == null) throw new AMMException(AMMErrorCodes.AMM_ZERO_ADDRESS, "Slot/venue");
        AMMVenue v = venuesById.get(venueId);
        if (v == null) throw new AMMException(AMMErrorCodes.AMM_VENUE_NOT_FOUND, "Venue");
        List<String> slots = slotIdsByVenue.get(venueId);
        if (slots != null && slots.size() >= AMMConstants.AMM_MAX_SLOTS_PER_VENUE)
            throw new AMMException(AMMErrorCodes.AMM_MAX_SLOTS, "Max slots");
        if (endEpoch <= startEpoch) throw new AMMException(AMMErrorCodes.AMM_INVALID_DURATION, "Duration");
        if (slotsById.containsKey(slotId)) throw new AMMException(AMMErrorCodes.AMM_BOOKING_EXISTS, "Slot exists");

        reentrancyLock = 1;
        try {
            AMMSlot s = new AMMSlot(slotId, venueId, startEpoch, endEpoch, sender);
            slotsById.put(slotId, s);
            if (slots != null) slots.add(slotId);
            long now = Instant.now().getEpochSecond();
            if (slotListedEvents.size() < AMM_MAX_EVENTS)
                slotListedEvents.add(new AMMSlotListed(slotId, venueId, startEpoch, endEpoch, now));
            return slotId;
        } finally {
            reentrancyLock = 0;
        }
    }

    public AMMSlot getSlot(String slotId) {
        return slotsById.get(slotId);
    }

    public List<String> getSlotIdsByVenue(String venueId) {
        List<String> list = slotIdsByVenue.get(venueId);
        return list != null ? new ArrayList<>(list) : Collections.emptyList();
    }

    // -------------------------------------------------------------------------
    // BOOKINGS
    // -------------------------------------------------------------------------

    public synchronized String bookTour(String guest, String slotId, BigDecimal amountWei) {
        requireNotReentrant();
        requireNotFrozen();
        if (guest == null || guest.isEmpty()) throw new AMMException(AMMErrorCodes.AMM_ZERO_ADDRESS, "Guest");
        if (amountWei == null || amountWei.signum() <= 0)
            throw new AMMException(AMMErrorCodes.AMM_ZERO_AMOUNT, "Amount");
        AMMSlot slot = slotsById.get(slotId);
        if (slot == null) throw new AMMException(AMMErrorCodes.AMM_SLOT_NOT_FOUND, "Slot");
        if (slot.getStatus() != AMMSlotStatus.OPEN)
            throw new AMMException(AMMErrorCodes.AMM_SLOT_UNAVAILABLE, "Unavailable");

        List<String> userBookings = bookingIdsByGuest.get(guest);
        if (userBookings != null && userBookings.size() >= AMMConstants.AMM_MAX_BOOKINGS_PER_USER)
            throw new AMMException(AMMErrorCodes.AMM_MAX_BOOKINGS, "Max bookings");

        reentrancyLock = 1;
        try {
            String bookingId = "bk-" + slotId + "-" + Instant.now().getEpochSecond();
            BigDecimal fee = amountWei.multiply(config.getFeeBps(), AMMConstants.MC)
                    .divide(new BigDecimal("10000", AMMConstants.MC), AMMConstants.MC);
            totalFeesCollected = totalFeesCollected.add(fee, AMMConstants.MC);

            AMMBooking b = new AMMBooking(bookingId, slotId, guest, slot.getGuideAddr(), amountWei, Instant.now().getEpochSecond());
            bookingsById.put(bookingId, b);
            slot.setStatus(AMMSlotStatus.BOOKED);
            bookingIdsByGuest.computeIfAbsent(guest, k -> new ArrayList<>()).add(bookingId);

            if (tourBookedEvents.size() < AMM_MAX_EVENTS)
                tourBookedEvents.add(new AMMTourBooked(bookingId, slotId, guest, slot.getGuideAddr(), amountWei, Instant.now().getEpochSecond()));
            return bookingId;
        } finally {
            reentrancyLock = 0;
        }
    }

    public AMMBooking getBooking(String bookingId) {
        return bookingsById.get(bookingId);
    }

    public List<String> getBookingIdsByGuest(String guest) {
        List<String> list = bookingIdsByGuest.get(guest);
        return list != null ? new ArrayList<>(list) : Collections.emptyList();
    }

    public void cancelBooking(String sender, String bookingId) {
        requireNotReentrant();
        AMMBooking b = bookingsById.get(bookingId);
        if (b == null) throw new AMMException(AMMErrorCodes.AMM_BOOKING_NOT_FOUND, "Booking");
        if (!b.getGuest().equals(sender) && !b.getGuide().equals(sender) && !curator.equals(sender) && !backupCurator.equals(sender))
            throw new AMMException(AMMErrorCodes.AMM_NOT_GUIDE, "Not participant");
        b.setStatus(AMMBookingStatus.CANCELLED);
        AMMSlot slot = slotsById.get(b.getSlotId());
        if (slot != null) slot.setStatus(AMMSlotStatus.OPEN);
    }

    // -------------------------------------------------------------------------
    // MESSAGING (optional)
    // -------------------------------------------------------------------------

    public synchronized String sendMessage(String fromAddr, String toAddr, String contentHash) {
        requireNotReentrant();
        requireMessagingEnabled();
        if (fromAddr == null || toAddr == null) throw new AMMException(AMMErrorCodes.AMM_ZERO_ADDRESS, "Address");
        if (contentHash == null || contentHash.isEmpty()) throw new AMMException(AMMErrorCodes.AMM_ZERO_AMOUNT, "Hash");

        String threadKey = fromAddr.compareTo(toAddr) < 0 ? fromAddr + ":" + toAddr : toAddr + ":" + fromAddr;
        String threadId = threadByParticipantPair.get(threadKey);
        if (threadId == null) {
            if (messageIdsByThread.size() >= AMMConstants.AMM_MAX_THREADS)
                throw new AMMException(AMMErrorCodes.AMM_MAX_MESSAGES, "Max threads");
            threadId = "th-" + Instant.now().getEpochSecond() + "-" + Math.abs(threadKey.hashCode());
            threadByParticipantPair.put(threadKey, threadId);
            messageIdsByThread.put(threadId, new ArrayList<>());
        }
        List<String> msgs = messageIdsByThread.get(threadId);
        if (msgs != null && msgs.size() >= AMMConstants.AMM_MAX_MESSAGES_PER_THREAD)
            throw new AMMException(AMMErrorCodes.AMM_MAX_MESSAGES, "Max messages per thread");

        reentrancyLock = 1;
        try {
            String messageId = "msg-" + threadId + "-" + Instant.now().getEpochSecond();
            AMMMessage m = new AMMMessage(messageId, threadId, fromAddr, toAddr, contentHash, Instant.now().getEpochSecond());
            messagesById.put(messageId, m);
            if (msgs != null) msgs.add(messageId);
            if (messageSentEvents.size() < AMM_MAX_EVENTS)
                messageSentEvents.add(new AMMMessageSent(threadId, fromAddr, toAddr, contentHash, Instant.now().getEpochSecond()));
            return messageId;
        } finally {
            reentrancyLock = 0;
        }
    }

    public AMMMessage getMessage(String messageId) {
        return messagesById.get(messageId);
    }

    public List<String> getMessageIdsByThread(String threadId) {
        List<String> list = messageIdsByThread.get(threadId);
        return list != null ? new ArrayList<>(list) : Collections.emptyList();
    }

    public String getThreadId(String addr1, String addr2) {
        String key = addr1.compareTo(addr2) < 0 ? addr1 + ":" + addr2 : addr2 + ":" + addr1;
        return threadByParticipantPair.get(key);
    }

    // -------------------------------------------------------------------------
    // CONFIG (curator only)
    // -------------------------------------------------------------------------

    public synchronized void setFeeBps(String sender, BigDecimal feeBps) {
        requireCurator(sender);
        if (feeBps == null || feeBps.signum() < 0 || feeBps.compareTo(new BigDecimal(AMMConstants.AMM_FEE_BPS_CAP)) > 0)
            throw new AMMException(AMMErrorCodes.AMM_INVALID_FEE, "Fee");
        this.config = new AMMConfig(feeBps, config.isMessagingEnabled(), config.isNamespaceFrozen());
    }

    public synchronized void setMessagingEnabled(String sender, boolean enabled) {
        requireCurator(sender);
        this.config = new AMMConfig(config.getFeeBps(), enabled, config.isNamespaceFrozen());
    }

    public synchronized void setNamespaceFrozen(String sender, boolean frozen) {
        requireCurator(sender);
        this.config = new AMMConfig(config.getFeeBps(), config.isMessagingEnabled(), frozen);
    }

    // -------------------------------------------------------------------------
    // EVENTS & STATS
    // -------------------------------------------------------------------------

    public List<AMMVenueAdded> getVenueAddedEvents() { return new ArrayList<>(venueAddedEvents); }
    public List<AMMSlotListed> getSlotListedEvents() { return new ArrayList<>(slotListedEvents); }
    public List<AMMTourBooked> getTourBookedEvents() { return new ArrayList<>(tourBookedEvents); }
    public List<AMMMessageSent> getMessageSentEvents() { return new ArrayList<>(messageSentEvents); }
    public BigDecimal getTotalFeesCollected() { return totalFeesCollected; }
    public int getBookingCount() { return bookingsById.size(); }
    public int getMessageCount() { return messagesById.size(); }

    // -------------------------------------------------------------------------
    // VENUE BY TYPE & DISTRICT
    // -------------------------------------------------------------------------

    public List<AMMVenue> listVenuesByType(AMMVenueType venueType) {
        return venuesById.values().stream()
                .filter(v -> v.getVenueType() == venueType)
                .collect(Collectors.toList());
    }

    public List<AMMSlot> getAvailableSlotsForVenue(String venueId) {
        List<String> ids = slotIdsByVenue.get(venueId);
        if (ids == null) return Collections.emptyList();
        return ids.stream()
                .map(slotsById::get)
                .filter(Objects::nonNull)
                .filter(s -> s.getStatus() == AMMSlotStatus.OPEN)
                .collect(Collectors.toList());
    }

    public List<AMMSlot> getOpenSlotsInRange(long fromEpoch, long toEpoch) {
        return slotsById.values().stream()
                .filter(s -> s.getStatus() == AMMSlotStatus.OPEN)
                .filter(s -> s.getStartEpoch() >= fromEpoch && s.getEndEpoch() <= toEpoch)
                .collect(Collectors.toList());
    }

    public void completeBooking(String sender, String bookingId) {
        requireNotReentrant();
        AMMBooking b = bookingsById.get(bookingId);
        if (b == null) throw new AMMException(AMMErrorCodes.AMM_BOOKING_NOT_FOUND, "Booking");
        if (!b.getGuide().equals(sender) && !curator.equals(sender) && !backupCurator.equals(sender))
            throw new AMMException(AMMErrorCodes.AMM_NOT_GUIDE, "Not guide");
        b.setStatus(AMMBookingStatus.COMPLETED);
    }

    public int getSlotCount() { return slotsById.size(); }
    public int getOpenSlotCount() {
        return (int) slotsById.values().stream().filter(s -> s.getStatus() == AMMSlotStatus.OPEN).count();
    }

    public List<AMMBooking> getBookingsByGuide(String guide) {
        return bookingsById.values().stream()
                .filter(b -> guide.equals(b.getGuide()))
                .collect(Collectors.toList());
    }

    public List<AMMBooking> getBookingsByStatus(AMMBookingStatus status) {
        return bookingsById.values().stream()
                .filter(b -> b.getStatus() == status)
                .collect(Collectors.toList());
    }

    public Optional<AMMVenue> findVenueByName(String name) {
        return venuesById.values().stream()
                .filter(v -> name != null && name.equals(v.getName()))
                .findFirst();
    }

    public List<String> listAllSlotIds() {
        return new ArrayList<>(slotsById.keySet());
    }

    public List<String> listAllBookingIds() {
        return new ArrayList<>(bookingsById.keySet());
    }

    public BigDecimal getFeesCollected() {
        return totalFeesCollected;
    }

    public boolean isCurator(String addr) {
        return curator.equals(addr) || backupCurator.equals(addr);
    }

    public boolean isMessagingEnabled() {
        return config.isMessagingEnabled();
    }

    public boolean isNamespaceFrozen() {
        return config.isNamespaceFrozen();
    }

    public void markMessageDelivered(String messageId) {
        AMMMessage m = messagesById.get(messageId);
        if (m != null) m.setStatus(AMMMessageStatus.DELIVERED);
    }

    public void markMessageRead(String messageId) {
        AMMMessage m = messagesById.get(messageId);
        if (m != null) m.setStatus(AMMMessageStatus.READ);
    }

    public List<AMMMessage> getMessagesInThread(String threadId) {
        List<String> ids = messageIdsByThread.get(threadId);
        if (ids == null) return Collections.emptyList();
        return ids.stream().map(messagesById::get).filter(Objects::nonNull).collect(Collectors.toList());
    }

    public int getThreadCount() {
        return threadByParticipantPair.size();
    }

    // -------------------------------------------------------------------------
    // BATCH LIST SLOTS
    // -------------------------------------------------------------------------

    public synchronized int batchListSlots(String sender, String venueId, List<SlotSpec> specs) {
        requireCurator(sender);
        requireNotReentrant();
        requireNotFrozen();
        AMMVenue v = venuesById.get(venueId);
        if (v == null) throw new AMMException(AMMErrorCodes.AMM_VENUE_NOT_FOUND, "Venue");
        List<String> slotList = slotIdsByVenue.get(venueId);
        if (slotList == null) return 0;
        int added = 0;
        long now = Instant.now().getEpochSecond();
        for (SlotSpec spec : specs) {
            if (slotList.size() >= AMMConstants.AMM_MAX_SLOTS_PER_VENUE) break;
            if (spec.slotId == null || slotsById.containsKey(spec.slotId)) continue;
            if (spec.endEpoch <= spec.startEpoch) continue;
            AMMSlot s = new AMMSlot(spec.slotId, venueId, spec.startEpoch, spec.endEpoch, sender);
            slotsById.put(spec.slotId, s);
            slotList.add(spec.slotId);
            added++;
            if (slotListedEvents.size() < AMM_MAX_EVENTS)
                slotListedEvents.add(new AMMSlotListed(spec.slotId, venueId, spec.startEpoch, spec.endEpoch, now));
        }
        return added;
    }

    public static final class SlotSpec {
        public final String slotId;
        public final long startEpoch;
        public final long endEpoch;
        public SlotSpec(String slotId, long startEpoch, long endEpoch) {
            this.slotId = slotId;
            this.startEpoch = startEpoch;
            this.endEpoch = endEpoch;
        }
    }

    // -------------------------------------------------------------------------
    // PAGINATION
    // -------------------------------------------------------------------------

    public List<AMMVenue> getVenuesPaginated(int offset, int limit) {
        List<AMMVenue> all = new ArrayList<>(venuesById.values());
        int from = Math.min(offset, all.size());
        int to = Math.min(from + limit, all.size());
        return all.subList(from, to);
    }

    public List<AMMSlot> getSlotsPaginated(int offset, int limit) {
        List<AMMSlot> all = new ArrayList<>(slotsById.values());
        int from = Math.min(offset, all.size());
        int to = Math.min(from + limit, all.size());
        return all.subList(from, to);
    }

    public List<AMMBooking> getBookingsPaginated(int offset, int limit) {
        List<AMMBooking> all = new ArrayList<>(bookingsById.values());
        int from = Math.min(offset, all.size());
        int to = Math.min(from + limit, all.size());
        return all.subList(from, to);
    }

    // -------------------------------------------------------------------------
    // AMSTERDAM DISTRICT / VENUE NAME REFERENCE (theme)
    // -------------------------------------------------------------------------

    public static final String AMM_DISTRICT_CENTRAL = "Centrum";
    public static final String AMM_DISTRICT_JORDAN = "Jordaan";
    public static final String AMM_DISTRICT_DE_PIJP = "De Pijp";
    public static final String AMM_DISTRICT_WEST = "West";
    public static final String AMM_DISTRICT_EAST = "Oost";
    public static final String AMM_DISTRICT_NORTH = "Noord";
    public static final String AMM_DISTRICT_SOUTH = "Zuid";
    public static final String AMM_CANAL_RING = "Grachtengordel";
    public static final String AMM_DEFAULT_VENUE_NAME = "Canal House";
    public static final String AMM_DEFAULT_LOUNGE_NAME = "Lounge";
    public static final String AMM_DEFAULT_STUDIO_NAME = "Private Studio";
    public static final int AMM_DEFAULT_SLOT_DURATION_SEC = 3600;
    public static final int AMM_MAX_SLOT_DURATION_SEC = 86400;

    public List<String> getDistrictList() {
        return Arrays.asList(AMM_DISTRICT_CENTRAL, AMM_DISTRICT_JORDAN, AMM_DISTRICT_DE_PIJP,
                AMM_DISTRICT_WEST, AMM_DISTRICT_EAST, AMM_DISTRICT_NORTH, AMM_DISTRICT_SOUTH, AMM_CANAL_RING);
    }

    public String suggestVenueName(AMMVenueType type) {
        switch (type) {
            case CANAL_HOUSE: return AMM_DEFAULT_VENUE_NAME;
            case LOUNGE: return AMM_DEFAULT_LOUNGE_NAME;
            case PRIVATE_STUDIO: return AMM_DEFAULT_STUDIO_NAME;
            case EXPERIENCE_ROOM: return "Experience Room";
            default: return "Venue";
        }
    }

    // -------------------------------------------------------------------------
    // VALIDATION HELPERS
    // -------------------------------------------------------------------------

    public static boolean isValidAddress(String addr) {
        if (addr == null || addr.length() != 42) return false;
        if (!addr.startsWith("0x")) return false;
        String hex = addr.substring(2);
        return hex.matches("[0-9a-fA-F]{40}");
    }

    public void requireValidAddress(String addr) {
        if (!isValidAddress(addr)) throw new AMMException(AMMErrorCodes.AMM_ZERO_ADDRESS, "Invalid address");
    }

    public boolean venueExists(String venueId) {
        return venuesById.containsKey(venueId);
    }

    public boolean slotExists(String slotId) {
        return slotsById.containsKey(slotId);
    }

    public boolean bookingExists(String bookingId) {
        return bookingsById.containsKey(bookingId);
    }

    public boolean messageExists(String messageId) {
        return messagesById.containsKey(messageId);
    }

    // -------------------------------------------------------------------------
    // SUMMARY / STATS
    // -------------------------------------------------------------------------

    public Map<String, Object> getEngineSummary() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("curator", curator);
        m.put("treasury", treasury);
        m.put("messageRelay", messageRelay);
        m.put("feeCollector", feeCollector);
        m.put("backupCurator", backupCurator);
        m.put("deployEpoch", deployEpoch);
        m.put("venueCount", venuesById.size());
        m.put("slotCount", slotsById.size());
        m.put("bookingCount", bookingsById.size());
        m.put("messageCount", messagesById.size());
        m.put("threadCount", threadByParticipantPair.size());
        m.put("totalFeesCollected", totalFeesCollected);
        m.put("feeBps", config.getFeeBps());
        m.put("messagingEnabled", config.isMessagingEnabled());
        m.put("namespaceFrozen", config.isNamespaceFrozen());
        return m;
    }

    public int getVenueCountByType(AMMVenueType type) {
        return (int) venuesById.values().stream().filter(v -> v.getVenueType() == type).count();
    }

    public int getBookingCountByGuest(String guest) {
        List<String> list = bookingIdsByGuest.get(guest);
        return list != null ? list.size() : 0;
    }

    public int getConfirmedBookingCount() {
        return (int) bookingsById.values().stream().filter(b -> b.getStatus() == AMMBookingStatus.CONFIRMED).count();
    }

    public int getCompletedBookingCount() {
        return (int) bookingsById.values().stream().filter(b -> b.getStatus() == AMMBookingStatus.COMPLETED).count();
    }

    // -------------------------------------------------------------------------
    // IMMUTABLE ADDRESS LIST (for EVM integration)
    // -------------------------------------------------------------------------

    public List<String> getImmutablesList() {
        return Arrays.asList(curator, treasury, messageRelay, feeCollector, backupCurator);
    }

    public String getZeroAddress() {
        return "0x0000000000000000000000000000000000000000";
    }

    public long getDeployEpochSeconds() {
        return deployEpoch;
    }

    public BigDecimal getCurrentFeeBps() {
        return config.getFeeBps();
    }

    public int getMaxVenues() { return AMMConstants.AMM_MAX_VENUES; }
    public int getMaxSlotsPerVenue() { return AMMConstants.AMM_MAX_SLOTS_PER_VENUE; }
    public int getMaxBookingsPerUser() { return AMMConstants.AMM_MAX_BOOKINGS_PER_USER; }
    public int getMaxMessagesPerThread() { return AMMConstants.AMM_MAX_MESSAGES_PER_THREAD; }
    public int getMaxThreads() { return AMMConstants.AMM_MAX_THREADS; }
    public int getFeeBpsCap() { return AMMConstants.AMM_FEE_BPS_CAP; }

    // -------------------------------------------------------------------------
    // EXTENDED QUERIES
    // -------------------------------------------------------------------------

    public List<AMMSlot> getSlotsByVenueAndStatus(String venueId, AMMSlotStatus status) {
        List<String> ids = slotIdsByVenue.get(venueId);
        if (ids == null) return Collections.emptyList();
        return ids.stream()
                .map(slotsById::get)
                .filter(Objects::nonNull)
                .filter(s -> s.getStatus() == status)
                .collect(Collectors.toList());
    }

    public List<AMMBooking> getActiveBookingsForGuest(String guest) {
        List<String> ids = bookingIdsByGuest.get(guest);
        if (ids == null) return Collections.emptyList();
        return ids.stream()
                .map(bookingsById::get)
                .filter(Objects::nonNull)
                .filter(b -> b.getStatus() == AMMBookingStatus.CONFIRMED || b.getStatus() == AMMBookingStatus.PENDING)
                .collect(Collectors.toList());
    }

    public long getEpochSeconds() {
        return Instant.now().getEpochSecond();
    }

    public boolean isSlotAvailable(String slotId) {
        AMMSlot s = slotsById.get(slotId);
        return s != null && s.getStatus() == AMMSlotStatus.OPEN;
    }

    public String getNamespace() {
        return AMMConstants.AMM_NAMESPACE;
    }

    public List<AMMVenueType> getAllVenueTypes() {
        return Arrays.asList(AMMVenueType.values());
    }

    public List<AMMBookingStatus> getAllBookingStatuses() {
        return Arrays.asList(AMMBookingStatus.values());
    }

    public List<AMMSlotStatus> getAllSlotStatuses() {
        return Arrays.asList(AMMSlotStatus.values());
    }

    public int countVenuesByCurator(String curatorAddr) {
        return (int) venuesById.values().stream().filter(v -> curatorAddr.equals(v.getCurator())).count();
    }

    public List<AMMVenue> getVenuesByCurator(String curatorAddr) {
        return venuesById.values().stream()
                .filter(v -> curatorAddr.equals(v.getCurator()))
                .collect(Collectors.toList());
    }

    public List<String> getAllThreadIds() {
        return new ArrayList<>(messageIdsByThread.keySet());
    }

    public BigDecimal computeFeeForAmount(BigDecimal amountWei) {
        if (amountWei == null || amountWei.signum() <= 0) return BigDecimal.ZERO;
        return amountWei.multiply(config.getFeeBps(), AMMConstants.MC)
                .divide(new BigDecimal("10000", AMMConstants.MC), AMMConstants.MC);
    }

    public BigDecimal netAmountAfterFee(BigDecimal amountWei) {
        if (amountWei == null || amountWei.signum() <= 0) return BigDecimal.ZERO;
        return amountWei.subtract(computeFeeForAmount(amountWei), AMMConstants.MC);
    }

    // -------------------------------------------------------------------------
    // AMSTERDAM TOUR GUIDE REFERENCE (adult industry theme — text only)
    // -------------------------------------------------------------------------

    public static final String AMM_GUIDE_INTRO = "Canal-side discovery and optional messaging.";
    public static final String AMM_GUIDE_CENTRAL_TIP = "Centrum: central canal ring and historic core.";
    public static final String AMM_GUIDE_JORDAN_TIP = "Jordaan: narrow streets and local character.";
    public static final String AMM_GUIDE_DE_PIJP_TIP = "De Pijp: vibrant and diverse neighbourhood.";
    public static final String AMM_GUIDE_WEST_TIP = "West: residential and quieter options.";
    public static final String AMM_GUIDE_EAST_TIP = "Oost: creative and mixed-use areas.";
    public static final String AMM_GUIDE_NORTH_TIP = "Noord: across IJ, developing district.";
    public static final String AMM_GUIDE_SOUTH_TIP = "Zuid: business and upscale residential.";
    public static final String AMM_GUIDE_CANAL_TIP = "Grachtengordel: UNESCO canal ring.";
    public static final int AMM_TOUR_MIN_DURATION_MIN = 60;
    public static final int AMM_TOUR_MAX_DURATION_MIN = 480;
    public static final String AMM_VENUE_TYPE_CANAL_DESC = "Canal house venue.";
    public static final String AMM_VENUE_TYPE_LOUNGE_DESC = "Lounge setting.";
    public static final String AMM_VENUE_TYPE_STUDIO_DESC = "Private studio.";
    public static final String AMM_VENUE_TYPE_EXPERIENCE_DESC = "Experience room.";
    public static final String AMM_MESSAGING_OPTIONAL_NOTE = "Messaging is optional and can be disabled by curator.";
    public static final String AMM_BOOKING_CONFIRMED_MSG = "Booking confirmed.";
    public static final String AMM_BOOKING_CANCELLED_MSG = "Booking cancelled.";
    public static final String AMM_SLOT_OPEN_MSG = "Slot open.";
    public static final String AMM_SLOT_BOOKED_MSG = "Slot booked.";
    public static final String AMM_FEE_CAP_NOTE = "Fee is capped at 5% (500 bps).";
    public static final String AMM_MAX_VENUES_NOTE = "Max 384 venues per engine.";
    public static final String AMM_MAX_SLOTS_NOTE = "Max 96 slots per venue.";
    public static final String AMM_MAX_BOOKINGS_PER_USER_NOTE = "Max 24 active bookings per guest.";
    public static final String AMM_MAX_MESSAGES_PER_THREAD_NOTE = "Max 512 messages per thread.";
    public static final String AMM_MAX_THREADS_NOTE = "Max 2048 threads.";
    public static final String AMM_CURATOR_ROLE = "Curator can add venues, list slots, set fee and messaging.";
    public static final String AMM_GUIDE_ROLE = "Guide is the slot lister; can complete bookings.";
    public static final String AMM_GUEST_ROLE = "Guest books tours and can cancel own bookings.";
    public static final String AMM_TREASURY_ADDRESS_NOTE = "Treasury receives protocol fees when configured.";
    public static final String AMM_MESSAGE_RELAY_NOTE = "Message relay address for optional messaging.";
    public static final String AMM_FEE_COLLECTOR_NOTE = "Fee collector receives booking fees.";
    public static final String AMM_BACKUP_CURATOR_NOTE = "Backup curator has same rights as primary.";
    public static final String AMM_NAMESPACE_FROZEN_NOTE = "When frozen, no new venues or slots.";
    public static final String AMM_REENTRANCY_NOTE = "Engine uses reentrancy lock for safety.";
    public static final String AMM_EIP55_ADDRESSES_NOTE = "All addresses are 40 hex chars, EIP-55 style.";
    public static final String AMM_DEPLOY_EPOCH_NOTE = "Deploy epoch set at construction.";
    public static final String AMM_EVENTS_LIMIT_NOTE = "Last 256 events per type are retained.";
    public static final String AMM_VENUE_ADDED_EVENT = "AMMVenueAdded";
    public static final String AMM_SLOT_LISTED_EVENT = "AMMSlotListed";
    public static final String AMM_TOUR_BOOKED_EVENT = "AMMTourBooked";
    public static final String AMM_MESSAGE_SENT_EVENT = "AMMMessageSent";
    public static final String AMM_ERROR_PREFIX = "AMM_";
    public static final String AMM_NAMESPACE_ID = "amsta-matcha-xxx.v1";

    public List<String> getGuideTipsList() {
        return Arrays.asList(AMM_GUIDE_INTRO, AMM_GUIDE_CENTRAL_TIP, AMM_GUIDE_JORDAN_TIP,
                AMM_GUIDE_DE_PIJP_TIP, AMM_GUIDE_WEST_TIP, AMM_GUIDE_EAST_TIP,
                AMM_GUIDE_NORTH_TIP, AMM_GUIDE_SOUTH_TIP, AMM_GUIDE_CANAL_TIP);
    }

    public String getVenueTypeDescription(AMMVenueType type) {
        switch (type) {
            case CANAL_HOUSE: return AMM_VENUE_TYPE_CANAL_DESC;
            case LOUNGE: return AMM_VENUE_TYPE_LOUNGE_DESC;
            case PRIVATE_STUDIO: return AMM_VENUE_TYPE_STUDIO_DESC;
            case EXPERIENCE_ROOM: return AMM_VENUE_TYPE_EXPERIENCE_DESC;
            default: return "Venue";
        }
    }

    public List<String> getRoleDescriptions() {
        return Arrays.asList(AMM_CURATOR_ROLE, AMM_GUIDE_ROLE, AMM_GUEST_ROLE);
    }

    public List<String> getConfigNotes() {
        return Arrays.asList(AMM_FEE_CAP_NOTE, AMM_MAX_VENUES_NOTE, AMM_MAX_SLOTS_NOTE,
                AMM_MAX_BOOKINGS_PER_USER_NOTE, AMM_MAX_MESSAGES_PER_THREAD_NOTE, AMM_MAX_THREADS_NOTE,
                AMM_NAMESPACE_FROZEN_NOTE, AMM_EIP55_ADDRESSES_NOTE);
    }

    // -------------------------------------------------------------------------
    // ERROR CODE LIST
    // -------------------------------------------------------------------------

    public static List<String> getAllErrorCodes() {
        return Arrays.asList(
                AMMErrorCodes.AMM_ZERO_ADDRESS,
                AMMErrorCodes.AMM_ZERO_AMOUNT,
                AMMErrorCodes.AMM_VENUE_NOT_FOUND,
                AMMErrorCodes.AMM_SLOT_NOT_FOUND,
                AMMErrorCodes.AMM_BOOKING_NOT_FOUND,
                AMMErrorCodes.AMM_MESSAGE_NOT_FOUND,
                AMMErrorCodes.AMM_NOT_CURATOR,
                AMMErrorCodes.AMM_NOT_GUIDE,
                AMMErrorCodes.AMM_SLOT_UNAVAILABLE,
                AMMErrorCodes.AMM_BOOKING_EXISTS,
                AMMErrorCodes.AMM_MAX_VENUES,
                AMMErrorCodes.AMM_MAX_SLOTS,
                AMMErrorCodes.AMM_MAX_BOOKINGS,
                AMMErrorCodes.AMM_MAX_MESSAGES,
                AMMErrorCodes.AMM_NAMESPACE_FROZEN,
                AMMErrorCodes.AMM_REENTRANT,
                AMMErrorCodes.AMM_INVALID_FEE,
                AMMErrorCodes.AMM_INVALID_DURATION,
                AMMErrorCodes.AMM_MESSAGE_DISABLED
        );
    }

    public String describeError(String code) {
        return AMMErrorCodes.describe(code);
    }

    // -------------------------------------------------------------------------
    // CONSTANT ACCESSORS
    // -------------------------------------------------------------------------

    public static int getConstantMaxVenues() { return AMMConstants.AMM_MAX_VENUES; }
    public static int getConstantMaxSlotsPerVenue() { return AMMConstants.AMM_MAX_SLOTS_PER_VENUE; }
    public static int getConstantMaxBookingsPerUser() { return AMMConstants.AMM_MAX_BOOKINGS_PER_USER; }
    public static int getConstantMaxMessagesPerThread() { return AMMConstants.AMM_MAX_MESSAGES_PER_THREAD; }
    public static int getConstantMaxThreads() { return AMMConstants.AMM_MAX_THREADS; }
    public static int getConstantFeeBpsCap() { return AMMConstants.AMM_FEE_BPS_CAP; }
    public static String getConstantNamespace() { return AMMConstants.AMM_NAMESPACE; }

    // -------------------------------------------------------------------------
    // EXTENDED HELPERS & SAFE LAUNCH
    // -------------------------------------------------------------------------

    public boolean canAddVenue() {
        return !config.isNamespaceFrozen() && venuesById.size() < AMMConstants.AMM_MAX_VENUES;
    }

    public boolean canListSlot(String venueId) {
        if (config.isNamespaceFrozen()) return false;
        List<String> list = slotIdsByVenue.get(venueId);
        return list != null && list.size() < AMMConstants.AMM_MAX_SLOTS_PER_VENUE;
    }

    public boolean canBook(String guest) {
        if (config.isNamespaceFrozen()) return false;
        List<String> list = bookingIdsByGuest.get(guest);
        int count = list != null ? list.size() : 0;
        return count < AMMConstants.AMM_MAX_BOOKINGS_PER_USER;
    }

    public boolean canSendMessage() {
        return config.isMessagingEnabled() && !config.isNamespaceFrozen();
    }

    public int remainingVenueSlots() {
        int c = venuesById.size();
        return c >= AMMConstants.AMM_MAX_VENUES ? 0 : AMMConstants.AMM_MAX_VENUES - c;
    }

    public int remainingSlotSlots(String venueId) {
        List<String> list = slotIdsByVenue.get(venueId);
        if (list == null) return AMMConstants.AMM_MAX_SLOTS_PER_VENUE;
        int c = list.size();
        return c >= AMMConstants.AMM_MAX_SLOTS_PER_VENUE ? 0 : AMMConstants.AMM_MAX_SLOTS_PER_VENUE - c;
    }

    public int remainingBookingSlots(String guest) {
        List<String> list = bookingIdsByGuest.get(guest);
        int c = list != null ? list.size() : 0;
        return c >= AMMConstants.AMM_MAX_BOOKINGS_PER_USER ? 0 : AMMConstants.AMM_MAX_BOOKINGS_PER_USER - c;
    }

    public AMMSlot getSlotOrThrow(String slotId) {
        AMMSlot s = slotsById.get(slotId);
        if (s == null) throw new AMMException(AMMErrorCodes.AMM_SLOT_NOT_FOUND, "Slot not found");
        return s;
    }

    public AMMVenue getVenueOrThrow(String venueId) {
        AMMVenue v = venuesById.get(venueId);
        if (v == null) throw new AMMException(AMMErrorCodes.AMM_VENUE_NOT_FOUND, "Venue not found");
        return v;
    }

    public AMMBooking getBookingOrThrow(String bookingId) {
        AMMBooking b = bookingsById.get(bookingId);
        if (b == null) throw new AMMException(AMMErrorCodes.AMM_BOOKING_NOT_FOUND, "Booking not found");
        return b;
    }

    public AMMMessage getMessageOrThrow(String messageId) {
        AMMMessage m = messagesById.get(messageId);
        if (m == null) throw new AMMException(AMMErrorCodes.AMM_MESSAGE_NOT_FOUND, "Message not found");
        return m;
    }

    public List<AMMVenue> listVenuesSortedByName() {
        List<AMMVenue> list = new ArrayList<>(venuesById.values());
        list.sort(Comparator.comparing(AMMVenue::getName));
        return list;
    }

    public List<AMMSlot> listSlotsSortedByStart() {
        List<AMMSlot> list = new ArrayList<>(slotsById.values());
        list.sort(Comparator.comparingLong(AMMSlot::getStartEpoch));
        return list;
    }

    public List<AMMBooking> listBookingsSortedByCreated() {
        List<AMMBooking> list = new ArrayList<>(bookingsById.values());
        list.sort(Comparator.comparingLong(AMMBooking::getCreatedAtEpoch));
        return list;
    }

    public Set<String> getAllGuides() {
        return slotsById.values().stream().map(AMMSlot::getGuideAddr).collect(Collectors.toSet());
    }

    public Set<String> getAllGuests() {
        return new HashSet<>(bookingIdsByGuest.keySet());
    }

    public int getMessageCountInThread(String threadId) {
        List<String> list = messageIdsByThread.get(threadId);
        return list != null ? list.size() : 0;
    }

    public boolean hasThread(String addr1, String addr2) {
        return getThreadId(addr1, addr2) != null;
    }

    // -------------------------------------------------------------------------
    // AMSTERDAM VENUE NAME SUGGESTIONS (theme — adult industry tour guide)
    // -------------------------------------------------------------------------

    private static final String[] AMM_VENUE_NAME_SUGGESTIONS = {
        "Herengracht View", "Keizersgracht Suite", "Prinsengracht Room",
        "Jordaan Hideaway", "De Pijp Lounge", "Nine Streets Studio",
        "Canal House One", "Canal House Two", "Private Lounge A",
        "Private Lounge B", "Experience Room North", "Experience Room South",
        "Central Canal Suite", "West Side Studio", "East Side Lounge",
        "Noord Over IJ", "Zuid Premium", "Grachtengordel Classic",
        "Singel Corner", "Brouwersgracht View", "Leliegracht Suite",
        "Runstraat Room", "Berensstraat Lounge", "Wolvenstraat Studio",
        "Hartenstraat Venue", "Huidenstraat Room", "Reestraat Lounge",
        "Wijde Heisteeg Studio", "Nieuwe Spiegelstraat Venue",
        "Amstel View", "Magere Brug Suite", "Blauwbrug Room",
        "Skinny Bridge Lounge", "Rembrandtplein Studio", "Leidseplein Venue",
        "Dam Square Room", "Red Light District Lounge", "Wallen Studio",
        "Oudezijds Achterburgwal Venue", "Oudezijds Voorburgwal Room",
        "Warmoesstraat Lounge", "Nieuwezijds Studio", "Spuistraat Venue",
        "Haarlemmerstraat Room", "Haarlemmerdijk Lounge", "Elandsgracht Studio",
        "Lindengracht Venue", "Boomstraat Room", "Tweede Tuindwarsstraat Lounge",
        "Albert Cuyp Studio", "Ferdinand Bolstraat Venue", "Van Woustraat Room",
        "Sarphatipark Lounge", "Marie Heinekenplein Studio", "Daniël Stalpertstraat Venue",
        "Stadionweg Room", "Olympisch Stadion Lounge", "Apollolaan Studio",
        "Museumplein Venue", "Vondelpark Room", "Overtoom Lounge",
        "Jan Pieter Heijestraat Studio", "Kinkerstraat Venue", "Ten Katestraat Room",
        "De Clercqstraat Lounge", "Postjesweg Studio", "Sloterkade Venue",
        "IJburg Room", "Java-eiland Lounge", "KNSM-eiland Studio",
        "Oostelijke Eilanden Venue", "Oosterdok Room", "Entrepotdok Lounge",
        "NDSM Wharf Studio", "Noordelijke IJoever Venue", "Buiksloterweg Room",
        "Distelweg Lounge", "Meeuwenlaan Studio", "Volewijck Venue",
        "Banne Room", "Waterlandplein Lounge", "Purmerweg Studio"
    };

    public static int getVenueNameSuggestionCount() {
        return AMM_VENUE_NAME_SUGGESTIONS.length;
    }

    public static String getVenueNameSuggestion(int index) {
        if (index < 0 || index >= AMM_VENUE_NAME_SUGGESTIONS.length) return AMM_DEFAULT_VENUE_NAME;
        return AMM_VENUE_NAME_SUGGESTIONS[index];
    }

    public static List<String> getAllVenueNameSuggestions() {
