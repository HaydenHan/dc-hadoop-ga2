package com.dataeye.hadoop.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

public class DCJsonUtil {

	public static Gson gson = new Gson();

	public static Gson getGson() {
		return gson;
	}

	public static TypeToken tokenInteger = new TypeToken<Set<Integer>>() {
	};

	public static TypeToken tokenString = new TypeToken<Set<String>>() {
	};

	public static TypeToken tokenIntMap = new TypeToken<Map<Integer, Integer>>() {
	};

	public static TypeToken tokenIntFloatMap = new TypeToken<Map<Integer, Float>>() {
	};

	public static Set<Integer> jsonToIntegerSet(String json) {
		try {
			return gson.fromJson(json, tokenInteger.getType());
		} catch (Throwable t) {
			return null;
		}
	}

	public static Set<String> jsonToStringSet(String json) {
		try {
			return gson.fromJson(json, tokenString.getType());
		} catch (Throwable t) {
			return null;
		}
	}

	public static Map<Integer, Integer> jsonToIntegerMap(String json) {
		try {
			return gson.fromJson(json, tokenIntMap.getType());
		} catch (Throwable t) {
			return null;
		}
	}

	public static Map<Integer, Float> jsonToIntFloatMap(String json) {
		try {
			return gson.fromJson(json, tokenIntFloatMap.getType());
		} catch (Throwable t) {
			return null;
		}
	}

	public static <T> Set<T> jsonToSet(String json, T T) {
		try {
			return gson.fromJson(json, new TypeToken<Set<T>>() {
			}.getType());
		} catch (Throwable t) {
			return null;
		}
	}

	public static <T1, T2> Map<T1, T2> getMapFromJson(String json, TypeToken<Map<T1, T2>> type) {
		try {
			return gson.fromJson(json, type.getType());
		} catch (Throwable t) {
			return null;
		}
	}

	@Deprecated
	public static <T1, T2> Map<T1, T2> getMapFromJson(String json, T1 O, T2 P) {
		try {
			return gson.fromJson(json, new TypeToken<Map<T1, T2>>() {
			}.getType());
		} catch (Throwable t) {
			return null;
		}
	}

	public static void main(String[] args) {
		System.out.println(getGson().toJson(new HashMap<Integer, Integer>()));
	}
}
