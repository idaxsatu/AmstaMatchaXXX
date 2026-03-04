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
