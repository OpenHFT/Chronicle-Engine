package net.openhft.chronicle.engine;

import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Date;


public class Factor implements BytesMarshallable {

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
    public void readMarshallable(@NotNull Bytes bytes) throws IllegalStateException {
        openPDFlag = bytes.readByte();
        openUCFlag = bytes.readByte();
        openActiveEMFlag = bytes.readByte();
        openPastDueEMFlag = bytes.readByte();
        accountCloseFlag = bytes.readByte();
        missingPaperFlag = bytes.readByte();
        rMLAgreementCodeFlag = bytes.readByte();
        nMEAccountFlag = bytes.readByte();
        accountClassificationTypeValue = bytes.readByte();
        accountNumber = bytes.readUTFΔ();
        firm = bytes.readUTFΔ();
    }


    @Override
    public void writeMarshallable(@NotNull Bytes bytes) {
        bytes.writeByte(openPDFlag);
        bytes.writeByte(openUCFlag);
        bytes.writeByte(openActiveEMFlag);
        bytes.writeByte(openPastDueEMFlag);
        bytes.writeByte(accountCloseFlag);
        bytes.writeByte(missingPaperFlag);
        bytes.writeByte(rMLAgreementCodeFlag);
        bytes.writeByte(nMEAccountFlag);
        bytes.writeByte(accountClassificationTypeValue);
        bytes.writeUTFΔ(accountNumber);
        bytes.writeUTFΔ(firm);
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