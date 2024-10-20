package org.example.entities;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
/**
 * This is the Membershiplist  entity class
 */
public class MembershipList {

    public static ConcurrentHashMap<String, Member> members = new ConcurrentHashMap<>();
    public static List<String> memberNames = new CopyOnWriteArrayList<>();
//    public static Set<String> memberNames = new ConcurrentSkipListSet<>();;
    public static int pointer;
    private static final Logger logger = LoggerFactory.getLogger(MembershipList.class);

    public static void addMember(Member member) {
        members.put(member.getName(), member);
        if(!memberNames.contains(member.getName()))
            memberNames.add(member.getName());
    }

    public static void removeMember(String name) {
        members.remove(name);
        memberNames.remove(name);
    }

    public static void printMembers() {
//        System.out.println("Printing members at :" + FDProperties.getFDProperties().get("machineName"));
        members.forEach((k, v) -> {
            ObjectMapper mapper = new ObjectMapper();
            try {
                String json = mapper.writeValueAsString(v);
                logger.info(k + ": " + json);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static void printMembersId() {
        System.out.println("Printing members at : " + FDProperties.getFDProperties().get("machineName"));
        members.forEach((k, v) -> {
            ObjectMapper mapper = new ObjectMapper();
            try {
                String json = mapper.writeValueAsString(v);
                System.out.println(" " +  v.getName() + "_" + v.getIpAddress() + "_" + v.getPort() + "_" + v.getVersionNo() + "_" + v.getIncarnationNo());
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static List<Member> getSuspectedMembers() {
        List<Member> suspectedMembers = new ArrayList<>();
        members.forEach((k, v) -> {
            if(v.getStatus().equals("Suspected")) {
                suspectedMembers.add(v);
            }
        });
        return suspectedMembers;
    }

    public static void generateRandomList() {
        pointer = 0;
        Collections.shuffle(memberNames);
    }

    public static Member getRandomMember() {
        Member member = members.get(memberNames.get(pointer));
        pointer++;
        return member;
    }

    public static Boolean isLast(){
//        logger.debug(pointer + " " + memberNames.size() + " " + members.size());
        return pointer < memberNames.size();
    }

    public static List<Member> getKRandomMember(int k, String targetNode) {
        List<Member> memberList = new ArrayList<>();
        // Check if map is not empty
        if (!memberNames.isEmpty()) {
            // Get a random key and the corresponding value
            while (memberList.size() < k && memberList.size() < memberNames.size()) {
                //TODO now send ping to other k nodes
                Member member = members.get(memberNames.get(ThreadLocalRandom.current().nextInt(memberNames.size())));
                if (!memberList.contains(member) && targetNode.equals(member.getName())) {
                    Member newMember = new Member(member.getName(),
                            member.getIpAddress(),
                            member.getPort(),
                            member.getVersionNo(),
                            member.getStatus(),
                            member.getDateTime(),
                            member.getIncarnationNo());
                    memberList.add(newMember);
                }
            }
        } else {
            logger.info("Map is empty.");
        }
        return memberList;
    }
}
