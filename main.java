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
