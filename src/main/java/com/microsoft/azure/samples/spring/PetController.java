/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See LICENSE in the project root for
 * license information.
 */

package com.microsoft.azure.samples.spring;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.microsoft.azure.cosmos.cassandra.CosmosLoadBalancingPolicy;
import com.microsoft.azure.cosmos.cassandra.CosmosRetryPolicy;

import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.UUID;

@Controller
@RequestMapping(path = "/pets")
public class PetController {
    private static Configurations config = new Configurations();

    private static int PORT;
    private static String[] CONTACT_POINTS;
    static {
        String value;
        try {
            value = config.getProperty("spring.data.cassandra.port");
            PORT = Short.parseShort(value);
            value = config.getProperty("spring.data.cassandra.contact-points");
            CONTACT_POINTS = new String[] { value };
        } catch (IOException e) {
            e.printStackTrace();
        }
    };

    public PetController() throws Exception {
    }

    CassandraUtils utils = new CassandraUtils();

    //set retry policy defaults
    private static final int FIXED_BACK_OFF_TIME = 5000;
    private static final int GROWING_BACK_OFF_TIME = 1000;
    private static final int MAX_RETRY_COUNT = 20;
    public static final int NUMBER_OF_THREADS = 40;
    public static final int NUMBER_OF_WRITES_PER_THREAD = 5;

    //set Cosmos Retry Policy
    CosmosRetryPolicy retryPolicy = new CosmosRetryPolicy(MAX_RETRY_COUNT, FIXED_BACK_OFF_TIME, GROWING_BACK_OFF_TIME);

    //set load balancing policy to prefer reads and writes to West US region
    CosmosLoadBalancingPolicy loadBalancingPolicy = CosmosLoadBalancingPolicy.builder().withWriteDC("West US").withReadDC("West US").build();
    
    //create the Cassandra API session with custom policies loaded
    Session cassandraSession = utils.getSession(CONTACT_POINTS, PORT, retryPolicy, loadBalancingPolicy);

    //create spring data template with session
    CassandraOperations template = new CassandraTemplate(cassandraSession);

    @PostMapping
    public @ResponseBody String createPet(@RequestBody Pet pet) {
        pet.setId(UUIDs.timeBased());
        template.insert(pet);
        System.out.println("Added %s."+pet);
        return String.format("Added %s.", pet);
    }

    @GetMapping("/{id}")
    public @ResponseBody Pet getPet(@PathVariable UUID id) {
        return template.selectOneById(id, Pet.class);
    }

    @DeleteMapping("/{id}")
    public @ResponseBody String deletePet(@PathVariable UUID id) {
        template.deleteById(id, Pet.class);
        return "Deleted " + id;
    }
}
