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

package net.openhft.chronicle.engine.mit;

import org.junit.internal.builders.JUnit4Builder;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by peter.lawrey on 16/07/2015.
 */
public class AllTestMain {
    public static void main(String[] args) throws Throwable {
        System.out.println("Start tests on " + new Date());

        final List<Failure> failures = new ArrayList<>();
        RunNotifier runNotifier = new RunNotifier();
        runNotifier.addFirstListener(new RunListener() {
            @Override
            public void testFailure(Failure failure) {
                failures.add(failure);
                System.err.println(failure);
                failure.getException().printStackTrace();
            }
        });
        for (Class testClass : new Class[]{
//                FailingTest.class,
//                ReplicationTest.class,
                RemoteSubscriptionModelPerformanceTest.class,
//                SubscriptionModelTest.class,
                SubscriptionModelPerformanceTest.class,
                SubscriptionModelFilePerKeyPerformanceTest.class,
//                ManyMapsTest.class,
//                ManyMapsFilePerKeyTest.class
        }) {
            System.out.println("\n=====================");
            System.out.println("\t" + testClass);
            System.out.println("=====================");
            new JUnit4Builder().runnerForClass(testClass).run(runNotifier);
        }
//        if (failures.size() == 1) {
//            System.out.println("Got the expected number of failures, 1");
//        } else {
        if (!failures.isEmpty()) {
            System.out.println("\n" +
                    "***************************\n" +
                    "\tFAILED TESTS\n" +
                    "***************************");

            for (Failure failure : failures) {
                System.err.println(failure);
                failure.getException().printStackTrace();
            }
            System.exit(-1);
        }
        System.out.println("Finished tests on " + new Date());
    }
}
