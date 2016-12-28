/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.engine.mit;

import org.jetbrains.annotations.NotNull;
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

        @NotNull final List<Failure> failures = new ArrayList<>();
        @NotNull RunNotifier runNotifier = new RunNotifier();
        runNotifier.addFirstListener(new RunListener() {
            @Override
            public void testFailure(@NotNull Failure failure) {
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

            for (@NotNull Failure failure : failures) {
                System.err.println(failure);
                failure.getException().printStackTrace();
            }
            System.exit(-1);
        }
        System.out.println("Finished tests on " + new Date());
    }
}
