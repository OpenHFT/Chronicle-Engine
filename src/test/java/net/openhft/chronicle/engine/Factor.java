/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.engine;

import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Date;

public class Factor implements Marshallable, net.openhft.lang.io.serialization.BytesMarshallable {

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
    public void readMarshallable(@NotNull WireIn wireIn) throws IllegalStateException {
        wireIn.read(() -> "openPDFlag").int8(b -> openPDFlag = b);
        wireIn.read(() -> "openUCFlag").int8(b -> openUCFlag = b);
        wireIn.read(() -> "openActiveEMFlag").int8(b -> openActiveEMFlag = b);
        wireIn.read(() -> "openPastDueEMFlag").int8(b -> openPastDueEMFlag = b);
        wireIn.read(() -> "accountCloseFlag").int8(b -> accountCloseFlag = b);
        wireIn.read(() -> "missingPaperFlag").int8(b -> missingPaperFlag = b);
        wireIn.read(() -> "rMLAgreementCodeFlag").int8(b -> rMLAgreementCodeFlag = b);
        wireIn.read(() -> "nMEAccountFlag").int8(b -> nMEAccountFlag = b);
        wireIn.read(() -> "accountClassificationTypeValue").int8(b -> accountClassificationTypeValue = b);
        wireIn.read(() -> "accountNumber")
                .text(b -> accountNumber = b);
        wireIn.read(() -> "firm").text(b -> firm = b);
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
        wireOut.write(() -> "openPDFlag").int8(openPDFlag);
        wireOut.write(() -> "openUCFlag").int8(openUCFlag);
        wireOut.write(() -> "openActiveEMFlag").int8(openActiveEMFlag);
        wireOut.write(() -> "openPastDueEMFlag").int8(openPastDueEMFlag);
        wireOut.write(() -> "accountCloseFlag").int8(accountCloseFlag);
        wireOut.write(() -> "missingPaperFlag").int8(missingPaperFlag);
        wireOut.write(() -> "rMLAgreementCodeFlag").int8(rMLAgreementCodeFlag);
        wireOut.write(() -> "nMEAccountFlag").int8(nMEAccountFlag);
        wireOut.write(() -> "accountClassificationTypeValue").int8(accountClassificationTypeValue);
        wireOut.write(() -> "accountNumber")
                .text(accountNumber);
        wireOut.write(() -> "firm").text(firm);
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

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Factor factor = (Factor) o;

        if (openPDFlag != factor.openPDFlag) return false;
        if (openUCFlag != factor.openUCFlag) return false;
        if (openActiveEMFlag != factor.openActiveEMFlag) return false;
        if (openPastDueEMFlag != factor.openPastDueEMFlag) return false;
        if (accountCloseFlag != factor.accountCloseFlag) return false;
        if (missingPaperFlag != factor.missingPaperFlag) return false;
        if (rMLAgreementCodeFlag != factor.rMLAgreementCodeFlag) return false;
        if (nMEAccountFlag != factor.nMEAccountFlag) return false;
        if (accountClassificationTypeValue != factor.accountClassificationTypeValue) return false;
        if (accountNumber != null ? !accountNumber.equals(factor.accountNumber) : factor.accountNumber != null)
            return false;
        if (processDate != null ? !processDate.equals(factor.processDate) : factor.processDate != null) return false;
        return !(firm != null ? !firm.equals(factor.firm) : factor.firm != null);

    }

    @Override
    public int hashCode() {
        int result = (int) openPDFlag;
        result = 31 * result + (int) openUCFlag;
        result = 31 * result + (int) openActiveEMFlag;
        result = 31 * result + (int) openPastDueEMFlag;
        result = 31 * result + (int) accountCloseFlag;
        result = 31 * result + (int) missingPaperFlag;
        result = 31 * result + (int) rMLAgreementCodeFlag;
        result = 31 * result + (int) nMEAccountFlag;
        result = 31 * result + (int) accountClassificationTypeValue;
        result = 31 * result + (accountNumber != null ? accountNumber.hashCode() : 0);
        result = 31 * result + (processDate != null ? processDate.hashCode() : 0);
        result = 31 * result + (firm != null ? firm.hashCode() : 0);
        return result;
    }
}