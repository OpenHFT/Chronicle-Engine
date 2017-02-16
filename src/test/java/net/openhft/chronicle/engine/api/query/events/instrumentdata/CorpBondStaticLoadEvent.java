package net.openhft.chronicle.engine.api.query.events.instrumentdata;


import net.openhft.chronicle.wire.Marshallable;

import static net.openhft.chronicle.wire.WireType.TEXT;

/**
 * Represents a CorpBondStatic record that can be published to a Chronicle queue.
 */
public class CorpBondStaticLoadEvent extends CorpBondStaticKey implements Marshallable {

    private final double minimumPiece;

    public float getMinimumPiece() {
        return (float)minimumPiece;
    }

    public CorpBondStaticLoadEvent(int uiid,
                                   double minimumPiece) {
        super(uiid);
        this.minimumPiece = minimumPiece;
    }

    public final static class Builder {
        private final int uiid;
        private float minimumPiece;

        public Builder(int uiid) {
           this.uiid = uiid;
        }

        public CorpBondStaticLoadEvent build() {

            return new CorpBondStaticLoadEvent(
                    uiid,
                    minimumPiece);
        }

        public Builder minimumPiece(float minimumPiece) {
            this.minimumPiece = minimumPiece;
            return this;
        }
    }

    public static void main(String[] args) {
        // Create a CorpBondStaticLoadEvent, convert it to YAML, then convert it back to a CorpBondStaticLoadEvent
        String str = new Builder(123).build().toString();
        System.out.println(str);
        CorpBondStaticLoadEvent instance = TEXT.fromString(str);
        System.out.println("" + instance);
    }
}
