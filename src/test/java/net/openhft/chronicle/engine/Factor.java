package net.openhft.chronicle.engine;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Date;

public class Factor implements Marshallable, BytesMarshallable, net.openhft.lang.io.serialization.BytesMarshallable {

    private byte openPDFlag;
    private byte openUCFlag;
    private byte openActiveEMFlag;
    private byte openPastDueEMFlag;
    private byte accountCloseFlag;
    private byte missingPaperFlag;
    private byte rMLAgreementCodeFlag;
    private byte nMEAccountFlag;
    private byte accountClassificationTypeValue;
    @Nullable
    private String accountNumber;
    private Date processDate;
    @Nullable
    private String firm;

    @Nullable
    public String getAccountNumber() {
        return accountNumber;
    }

    public void setAccountNumber(String accountNumber) {
        this.accountNumber = accountNumber;
    }

    public byte getAccountClassificationTypeValue() {
        return accountClassificationTypeValue;
    }

    public void setAccountClassificationTypeValue(byte accountClassificationTypeValue) {
        this.accountClassificationTypeValue = accountClassificationTypeValue;
    }

    public byte getOpenPDFlag() {
        return openPDFlag;
    }

    public void setOpenPDFlag(byte openPDFlag) {
        this.openPDFlag = openPDFlag;
    }

    public byte getOpenUCFlag() {
        return openUCFlag;
    }

    public void setOpenUCFlag(byte openUCFlag) {
        this.openUCFlag = openUCFlag;
    }

    public byte getOpenActiveEMFlag() {
        return openActiveEMFlag;
    }

    public void setOpenActiveEMFlag(byte openActiveEMFlag) {
        this.openActiveEMFlag = openActiveEMFlag;
    }

    public byte getOpenPastDueEMFlag() {
        return openPastDueEMFlag;
    }

    public void setOpenPastDueEMFlag(byte openPastDueEMFlag) {
        this.openPastDueEMFlag = openPastDueEMFlag;
    }

    public byte getAccountCloseFlag() {
        return accountCloseFlag;
    }

    public void setAccountCloseFlag(byte accountCloseFlag) {
        this.accountCloseFlag = accountCloseFlag;
    }

    public byte getMissingPaperFlag() {
        return missingPaperFlag;
    }

    public void setMissingPaperFlag(byte missingPaperFlag) {
        this.missingPaperFlag = missingPaperFlag;
    }

    public byte getRMLAgreementCodeFlag() {
        return rMLAgreementCodeFlag;
    }

    public void setRMLAgreementCodeFlag(byte RMLAgreementCodeFlag) {
        this.rMLAgreementCodeFlag = RMLAgreementCodeFlag;
    }

    public byte getNMEAccountFlag() {
        return nMEAccountFlag;
    }

    public void setNMEAccountFlag(byte NMEAccountFlag) {
        this.nMEAccountFlag = NMEAccountFlag;
    }

    public Date getProcessDate() {
        return processDate;
    }

    public void setProcessDate(Date processDate) {
        this.processDate = processDate;
    }

    @Override
    public void readMarshallable(@NotNull net.openhft.lang.io.Bytes in) throws IllegalStateException {
        openPDFlag = in.readByte();
        openUCFlag = in.readByte();
        openActiveEMFlag = in.readByte();
        openPastDueEMFlag = in.readByte();
        accountCloseFlag = in.readByte();
        missingPaperFlag = in.readByte();
        rMLAgreementCodeFlag = in.readByte();
        nMEAccountFlag = in.readByte();
        accountClassificationTypeValue = in.readByte();
        accountNumber = in.readUTFΔ();
        firm = in.readUTFΔ();
    }

    @Override
    public void readMarshallable(Bytes in) throws IllegalStateException {
        openPDFlag = in.readByte();
        openUCFlag = in.readByte();
        openActiveEMFlag = in.readByte();
        openPastDueEMFlag = in.readByte();
        accountCloseFlag = in.readByte();
        missingPaperFlag = in.readByte();
        rMLAgreementCodeFlag = in.readByte();
        nMEAccountFlag = in.readByte();
        accountClassificationTypeValue = in.readByte();
        accountNumber = in.readUTFΔ();
        firm = in.readUTFΔ();
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) throws IllegalStateException {
        readMarshallable(wireIn.bytes());
    }

    @Override
    public void writeMarshallable(Bytes out) {
        out.writeByte(openPDFlag);
        out.writeByte(openUCFlag);
        out.writeByte(openActiveEMFlag);
        out.writeByte(openPastDueEMFlag);
        out.writeByte(accountCloseFlag);
        out.writeByte(missingPaperFlag);
        out.writeByte(rMLAgreementCodeFlag);
        out.writeByte(nMEAccountFlag);
        out.writeByte(accountClassificationTypeValue);
        out.writeUTFΔ(accountNumber);
        out.writeUTFΔ(firm);
    }

    @Override
    public void writeMarshallable(@NotNull net.openhft.lang.io.Bytes out) {
        out.writeByte(openPDFlag);
        out.writeByte(openUCFlag);
        out.writeByte(openActiveEMFlag);
        out.writeByte(openPastDueEMFlag);
        out.writeByte(accountCloseFlag);
        out.writeByte(missingPaperFlag);
        out.writeByte(rMLAgreementCodeFlag);
        out.writeByte(nMEAccountFlag);
        out.writeByte(accountClassificationTypeValue);
        out.writeUTFΔ(accountNumber);
        out.writeUTFΔ(firm);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        writeMarshallable(wireOut.bytes());
    }

    @NotNull
    @Override
    public String toString() {
        return "Factor{" +
                "openPDFlag=" + openPDFlag +
                ", openUCFlag=" + openUCFlag +
                ", openActiveEMFlag=" + openActiveEMFlag +
                ", openPastDueEMFlag=" + openPastDueEMFlag +
                ", accountCloseFlag=" + accountCloseFlag +
                ", missingPaperFlag=" + missingPaperFlag +
                ", rMLAgreementCodeFlag=" + rMLAgreementCodeFlag +
                ", nMEAccountFlag=" + nMEAccountFlag +
                ", accountClassificationTypeValue=" + accountClassificationTypeValue +
                ", accountNumber='" + accountNumber + '\'' +
                ", processDate=" + processDate +
                ", firm='" + firm + '\'' +
                '}';
    }

}