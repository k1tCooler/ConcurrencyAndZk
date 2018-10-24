package com.mmall.concurrency.example.zookeeper;

import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

public class AclUtils {
	
	public static String getDigestUserPwd(String id) throws Exception {
		return DigestAuthenticationProvider.generateDigest(id);
	}
}
