require("dotenv").config();
const { Client, GatewayIntentBits, Partials, EmbedBuilder } = require("discord.js");
const { DateTime } = require("luxon");

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.DirectMessages
  ],
  partials: [Partials.Channel, Partials.Message]
});

client.once("ready", () => {
  console.log(`Luciver is online as ${client.user.tag}`);
  startSchedulers();
});

const greetingIntroResponse = [
  "Hey there! I'm Luciver - your automation bot on call.",
  "Need backup? Say `Luciver help` for my command list or spell out the task with my name and I'll take it.",
  "Reminders, forwarding, cleanup, attendance recaps - point me at it and I'll keep things moving."
].join("\n");

const namePingResponse = "You mentioned my name - how can I help?";

const channelActivity = new Map();
const memberActivity = new Map();
const taskBacklog = [];
const reminderQueue = [];
const reachOutReports = [];
const MOD_DELETE_MAX_MESSAGES = 20;
const MOD_DELETE_MAX_RANGE_MESSAGES = 200;
const DISCORD_BULK_DELETE_WINDOW_MS = 14 * 24 * 60 * 60 * 1000;

const DAILY_REMINDER_ROLE_NAME = "bashers";
const DAILY_REMINDER_HOUR = 20; // 8 PM
const DAILY_REMINDER_MINUTE = 0;
const DAILY_REMINDER_MESSAGE = "Uploaded today's progress?! If not, do it now!!";

const MODERATOR_CHANNEL_ID = process.env.MODERATOR_CHANNEL_ID;
const LUCIVER_LOG_CHANNEL_ID = process.env.LUCIVER_LOG_CHANNEL_ID;
const TRACKED_VOICE_CHANNEL_NAMES = new Set(["voice meeting", "weekly-bash-discussion"]);
const TRACKED_VOICE_CHANNEL_IDS = new Set(
  (process.env.TRACKED_VOICE_CHANNEL_IDS || "")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean)
);
const REACH_OUT_CHANNEL_NAME = (process.env.REACH_OUT_CHANNEL_NAME || "reach-out").trim().toLowerCase();
const REACH_OUT_MAX_RECORDS = 500;

const voiceSessions = new Map();

// Voice session analytics (adjust via environment variables).
const VOICE_SESSION_BASE_DURATION_MS = Math.max(0, Number.parseFloat(process.env.VOICE_SESSION_BASE_DURATION_MINUTES || "60")) * 60 * 1000;
const VOICE_SESSION_MIN_LOG_DURATION_MS = Math.max(0, Number.parseFloat(process.env.VOICE_SESSION_MIN_LOG_DURATION_MINUTES || "5")) * 60 * 1000;
const VOICE_SESSION_FULL_ATTENDANCE_THRESHOLD = Math.min(1, Math.max(0, Number.parseFloat(process.env.VOICE_SESSION_FULL_ATTENDANCE_THRESHOLD || "0.95")));
const VOICE_SESSION_ON_TIME_THRESHOLD = Math.min(1, Math.max(0, Number.parseFloat(process.env.VOICE_SESSION_ON_TIME_THRESHOLD || "0.75")));

const TARGET_TIMEZONE = process.env.LUCIVER_TIMEZONE || "Asia/Kolkata";

const REMINDER_CHECK_INTERVAL_MS = 30 * 1000;
const TASK_DIGEST_CHECK_INTERVAL_MS = 15 * 60 * 1000;
const TASK_DIGEST_MIN_INTERVAL_MS = 6.5 * 24 * 60 * 60 * 1000;
const TASK_DIGEST_TARGET_DAY = 0; // Sunday
const TASK_DIGEST_TARGET_HOUR = 14; // 2 PM in configured timezone
const STATS_REPORT_MIN_INTERVAL_MS = 6.5 * 24 * 60 * 60 * 1000;
const STATS_REPORT_TARGET_DAY = 0; // Sunday
const STATS_REPORT_TARGET_HOUR = 18; // 6 PM in configured timezone

let lastTaskDigestSentAt = 0;
let lastStatsReportSentAt = 0;
let cachedLogChannel = null;
let dailyRoleReminderTimeout = null;

const recordChannelActivity = (message) => {
  if (!message.inGuild()) {
    return;
  }

  const now = Date.now();
  const key = message.channelId;
  const snapshot = channelActivity.get(key) || {
    count: 0,
    name: message.channel?.name || "unknown",
    updatedAt: 0
  };

  snapshot.count += 1;
  snapshot.name = message.channel?.name || snapshot.name;
  snapshot.updatedAt = now;

  channelActivity.set(key, snapshot);

  const authorId = message.author?.id;
  if (!authorId) {
    return;
  }

  const memberSnapshot = memberActivity.get(authorId) || {
    count: 0,
    tag: message.author.tag,
    updatedAt: 0,
    lastChannelId: message.channelId
  };

  memberSnapshot.count += 1;
  memberSnapshot.tag = message.author.tag;
  memberSnapshot.updatedAt = now;
  memberSnapshot.lastChannelId = message.channelId;

  memberActivity.set(authorId, memberSnapshot);
};

const normalizeForNameChecks = (text, botId) => {
  if (!text) {
    return "";
  }

  let working = text.trim().toLowerCase();

  if (botId) {
    const mentionPattern = new RegExp(`<@!?${botId}>`, "g");
    working = working.replace(mentionPattern, "luciver");
  }

  return working.replace(/[!?.:,*/\\`'"()\-]+/g, " ").replace(/\s+/g, " ").trim();
};

const isGreetingMessage = (message) => {
  const botId = client.user?.id || null;
  const normalized = normalizeForNameChecks(message.content, botId);
  if (!normalized || !normalized.includes("luciver")) {
    return false;
  }

  const tokens = normalized.split(" ").filter(Boolean);
  if (!tokens.includes("luciver")) {
    return false;
  }

  const greetingWords = new Set(["hi", "hello", "hey"]);
  const alphaTokens = tokens.filter((token) => /^[a-z]+$/.test(token) && token !== "luciver");
  if (!alphaTokens.length) {
    return true;
  }

  const hasGreeting = alphaTokens.some((token) => greetingWords.has(token));
  if (!hasGreeting) {
    return tokens.every((token) => token === "luciver");
  }

  return alphaTokens.every((token) => greetingWords.has(token));
};

const isPlainNamePing = (message) => {
  const botId = client.user?.id || null;
  const normalized = normalizeForNameChecks(message.content, botId);
  if (!normalized) {
    return false;
  }

  return normalized === "luciver";
};

const relativeTime = (timestamp) => {
  const diff = Date.now() - timestamp;
  if (diff < 60_000) {
    return "just now";
  }
  if (diff < 3_600_000) {
    const minutes = Math.round(diff / 60_000);
    return `${minutes} min ago`;
  }
  if (diff < 86_400_000) {
    const hours = Math.round(diff / 3_600_000);
    return `${hours} hr ago`;
  }
  const days = Math.round(diff / 86_400_000);
  return `${days} day${days === 1 ? "" : "s"} ago`;
};

const formatDuration = (milliseconds) => {
  if (!Number.isFinite(milliseconds) || milliseconds <= 0) {
    return "0s";
  }

  const totalSeconds = Math.round(milliseconds / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  const segments = [];
  if (hours) {
    segments.push(`${hours}h`);
  }
  if (minutes) {
    segments.push(`${minutes}m`);
  }
  if (!segments.length || seconds) {
    segments.push(`${seconds}s`);
  }

  return segments.join(" ");
};

const formatPercentageValue = (value) => {
  if (!Number.isFinite(value)) {
    return "0%";
  }

  const percentage = Math.round(value * 100);
  return `${percentage}%`;
};

const formatJoinOffsetBadge = (offsetMs) => {
  if (!Number.isFinite(offsetMs)) {
    return null;
  }

  if (offsetMs <= 60_000) {
    return "on-time";
  }

  const minutes = Math.round(offsetMs / 60_000);
  if (minutes < 60) {
    return `+${minutes}m`;
  }

  const hours = Math.round(offsetMs / 3_600_000);
  return `+${hours}h`;
};

// Splits attendance reports into chunks that stay below Discord's embed length limits.
const chunkLinesByLength = (lines, maxLength = 3500) => {
  if (!Array.isArray(lines) || !lines.length) {
    return [];
  }

  const chunks = [];
  let current = [];
  let currentLength = 0;

  lines.forEach((line) => {
    const lineLength = line.length;
    if (currentLength + lineLength + 1 > maxLength && current.length) {
      chunks.push(current.join("\n"));
      current = [];
      currentLength = 0;
    }

    current.push(line);
    currentLength += lineLength + 1;
  });

  if (current.length) {
    chunks.push(current.join("\n"));
  }

  return chunks;
};

const formatHelpField = (command, example, description) => {
  return [`- Command: ${command}`, `- Example: ${example}`, `- What it does: ${description}`].join("\n");
};

const buildHelpEmbed = () => {
  return new EmbedBuilder()
    .setAuthor({ name: "Hey, I got you here." })
    .setTitle("Luciver Quick Assist")
    .setColor(0x4f46e5)
    .setDescription("Mention me with one of these prompts and I’ll take it from there:")
    .addFields(
      {
        name: "Get Help",
        value: formatHelpField(
          "Luciver help",
          "Luciver help",
          "Shows every available feature with quick reminders so you can move fast."
        ),
        inline: false
      },
      {
        name: "Forward Something",
        value: formatHelpField(
          "Luciver ping #channel [note]",
          "Luciver ping #frontend send the above plan",
          "Relays the replied (or last non-bot) message into each tagged channel and keeps your note attached."
        ),
        inline: false
      },
      {
        name: "Assign Work (mods)",
        value: formatHelpField(
          "Luciver assign @teammate task details",
          "Luciver assign @Nova ship mobile polish by Friday",
          "Logs the task, DMs the assignee (channel fallback if needed), and queues it for the Sunday moderator digest."
        ),
        inline: false
      },
      {
        name: "Clear Recent Messages (mods)",
        value: formatHelpField(
          "Luciver delete/remove …",
          "Luciver delete the above 3 messages\nLuciver delete all messages from today up to 10am\nLuciver delete all messages from 14 Dec",
          "Tidies previous posts: remove up to 20 messages by count, sweep everything above, or clear today’s history up to a specific time (caps at 200 and skips items older than 14 days)."
        ),
        inline: false
      },
      {
        name: "Set a Reminder",
        value: formatHelpField(
          "Luciver remind me/@user/@everyone note in <time>",
          "Luciver remind @everyone prep release notes at 17:00",
          "Understands in 20m, tomorrow 5pm, or 17 Dec 09:00, then delivers the reminder directly at the right moment."
        ),
        inline: false
      },
      {
        name: "Server Pulse",
        value: formatHelpField(
          "Luciver stats",
          "Luciver stats",
          "Returns traffic totals, top channels, top contributors, quiet spots, and the ops snapshot (tasks, reminders, reach-outs)."
        ),
        inline: false
      },
      {
        name: "Reach-out Cover",
        value: formatHelpField(
          "@Luciver <reason> (inside reach-out)",
          "@Luciver I’m tied up with a client call, can’t make the 5 PM review",
          "Drop your reason in reach-out and I’ll log it for moderators, leave a trail in your CBS record, and spare you follow-up pings."
        ),
        inline: false
      },
      {
        name: "Automation Highlights",
        value: [
          "- Weekly digest lands Sundays 14:00 in the moderator channel (set MODERATOR_CHANNEL_ID).",
          "- Daily progress ping nudges the bashers role every evening at 20:00.",
          "- Voice meeting attendance summaries post automatically when tracked rooms empty."
        ].join("\n"),
        inline: false
      }
    )
    .setFooter({ text: "Need something custom? Say my name and describe it." });
};

const hasModeratorPrivileges = (message) => {
  if (!message.guild || !message.member) {
    return false;
  }

  const memberRoles = message.member.roles?.cache;
  if (!memberRoles?.size) {
    return false;
  }

  return memberRoles.some((role) =>
    typeof role.name === "string" && role.name.trim().toLowerCase().includes("moderator")
  );
};

const fetchLogChannel = async () => {
  if (!LUCIVER_LOG_CHANNEL_ID) {
    return null;
  }

  if (cachedLogChannel) {
    return cachedLogChannel;
  }

  try {
    const channel = await client.channels.fetch(LUCIVER_LOG_CHANNEL_ID);
    if (channel?.isTextBased()) {
      cachedLogChannel = channel;
      return channel;
    }
  } catch (error) {
    console.error("Failed to fetch log channel", error);
  }
  return null;
};

const postLogEntry = async (content, options = {}) => {
  const channel = await fetchLogChannel();
  if (!channel) {
    return;
  }

  try {
    await channel.send({
      content,
      embeds: options.embeds,
      allowedMentions: options.allowedMentions || { users: [], roles: [] }
    });
  } catch (error) {
    console.error("Failed to send log entry", error);
  }
};

const fetchMemberRoleCounts = async (guild) => {
  if (!guild) {
    return null;
  }

  try {
    await guild.members.fetch();
  } catch (error) {
    console.warn("Unable to fetch all members for role snapshot", error);
  }

  let moderators = 0;
  let others = 0;

  guild.members.cache.forEach((member) => {
    if (member.user?.bot) {
      return;
    }

    const isModerator = member.roles?.cache?.some((role) =>
      typeof role.name === "string" && role.name.trim().toLowerCase().includes("moderator")
    );

    if (isModerator) {
      moderators += 1;
    } else {
      others += 1;
    }
  });

  return { moderators, others };
};

const formatRoleSnapshot = (roleStats) => {
  if (!roleStats) {
    return null;
  }

  return `Moderators: ${roleStats.moderators} | Others: ${roleStats.others}`;
};

const resolveVoiceChannelName = (channel) => channel?.name || `voice-${channel?.id ?? "unknown"}`;

const isTrackedVoiceChannel = (channel) => {
  if (!channel) {
    return false;
  }

  if (channel.id && TRACKED_VOICE_CHANNEL_IDS.has(channel.id)) {
    return true;
  }

  if (channel.name) {
    return TRACKED_VOICE_CHANNEL_NAMES.has(channel.name.trim().toLowerCase());
  }

  return false;
};

const ensureVoiceSession = (channel) => {
  if (!channel) {
    return null;
  }

  let session = voiceSessions.get(channel.id);
  if (!session) {
    session = {
      channelId: channel.id,
      channelName: resolveVoiceChannelName(channel),
      startedAt: Date.now(),
      participants: new Map(),
      activeCount: 0,
      peakCount: 0
    };
    voiceSessions.set(channel.id, session);
  }

  return session;
};

const getParticipantLabel = (state) => {
  const member = state?.member;
  if (member?.displayName) {
    return member.displayName;
  }

  const user = member?.user || state?.user;
  if (user?.tag) {
    return user.tag;
  }

  return `User ${state?.id ?? "unknown"}`;
};

const markParticipantJoin = (session, voiceState) => {
  if (!session || !voiceState) {
    return;
  }

  const userId = voiceState.id;
  const now = Date.now();
  const displayName = getParticipantLabel(voiceState);

  let participant = session.participants.get(userId);
  if (!participant) {
    participant = {
      userId,
      displayName,
      totalMs: 0,
      lastJoinAt: null,
      firstJoinAt: now
    };
    session.participants.set(userId, participant);
  } else {
    participant.displayName = displayName;
    if (participant.firstJoinAt == null) {
      participant.firstJoinAt = now;
    }
  }

  if (participant.lastJoinAt == null) {
    participant.lastJoinAt = now;
    session.activeCount += 1;
    session.peakCount = Math.max(session.peakCount, session.activeCount);
  }
};

const markParticipantLeave = (session, voiceState) => {
  if (!session || !voiceState) {
    return;
  }

  const userId = voiceState.id;
  const participant = session.participants.get(userId);
  if (!participant) {
    return;
  }

  if (participant.lastJoinAt != null) {
    const now = Date.now();
    participant.totalMs += Math.max(0, now - participant.lastJoinAt);
    participant.lastJoinAt = null;
    session.activeCount = Math.max(0, session.activeCount - 1);
  }
};

const finalizeVoiceSession = async (channelId) => {
  const session = voiceSessions.get(channelId);
  if (!session) {
    return;
  }

  const now = Date.now();

  session.participants.forEach((participant) => {
    if (participant.lastJoinAt != null) {
      participant.totalMs += Math.max(0, now - participant.lastJoinAt);
      participant.lastJoinAt = null;
    }
  });

  const durationMs = Math.max(0, now - session.startedAt);
  const baseDurationMs = VOICE_SESSION_BASE_DURATION_MS || durationMs;

  if (durationMs < VOICE_SESSION_MIN_LOG_DURATION_MS && VOICE_SESSION_MIN_LOG_DURATION_MS > 0) {
    await postLogEntry(
      `Voice session summary skipped — <#${session.channelId}> wrapped in ${formatDuration(durationMs)}, below the ${formatDuration(VOICE_SESSION_MIN_LOG_DURATION_MS)} minimum window.`,
      { allowedMentions: { users: [], roles: [] } }
    );
    voiceSessions.delete(channelId);
    return;
  }

  const participantsRaw = [...session.participants.values()].filter((participant) => participant.totalMs > 0);
  const participantCount = participantsRaw.length;

  if (!participantCount) {
    voiceSessions.delete(channelId);
    return;
  }

  const totalAttendanceMs = participantsRaw.reduce((sum, participant) => sum + participant.totalMs, 0);
  const averageConcurrent = durationMs > 0 ? totalAttendanceMs / durationMs : 0;
  const averageConcurrentLabel = averageConcurrent ? averageConcurrent.toFixed(1).replace(/\.0$/, "") : "0";
  const overtimeMs = Math.max(0, durationMs - baseDurationMs);
  const peakConcurrent = Math.max(session.peakCount || 0, participantCount);

  const participants = participantsRaw
    .map((participant) => {
      const attendanceFraction = durationMs > 0 ? Math.min(1, participant.totalMs / durationMs) : 0;
      const joinOffset = Number.isFinite(participant.firstJoinAt)
        ? Math.max(0, participant.firstJoinAt - session.startedAt)
        : null;
      const badges = [];

      if (attendanceFraction >= VOICE_SESSION_FULL_ATTENDANCE_THRESHOLD) {
        badges.push("full");
      } else if (attendanceFraction >= VOICE_SESSION_ON_TIME_THRESHOLD) {
        badges.push("steady");
      }

      if (baseDurationMs > 0 && participant.totalMs >= baseDurationMs) {
        badges.push("overtime");
      }

      const offsetBadge = formatJoinOffsetBadge(joinOffset);
      if (offsetBadge) {
        badges.push(offsetBadge);
      }

      return {
        ...participant,
        attendanceFraction,
        joinOffset,
        badges
      };
    })
    .sort((a, b) => b.totalMs - a.totalMs);

  const fullAttendanceCount = participants.filter(
    (participant) => participant.attendanceFraction >= VOICE_SESSION_FULL_ATTENDANCE_THRESHOLD
  ).length;
  const steadyAttendanceCount = participants.filter(
    (participant) => participant.attendanceFraction >= VOICE_SESSION_ON_TIME_THRESHOLD
  ).length;

  const leaders = participants.slice(0, Math.min(3, participants.length));
  const firstArrivals = participants
    .filter((participant) => Number.isFinite(participant.joinOffset))
    .slice()
    .sort((a, b) => (a.joinOffset ?? Infinity) - (b.joinOffset ?? Infinity))
    .slice(0, Math.min(3, participants.length));

  const rankWidth = String(participants.length).length;
  const attendanceLines = participants.map((participant, index) => {
    const rankLabel = String(index + 1).padStart(rankWidth, " ");
    const percentLabel = formatPercentageValue(participant.attendanceFraction);
    const badgeText = participant.badges.length
      ? ` ${participant.badges.map((badge) => `[${badge}]`).join("")}`
      : "";
    return `${rankLabel}. <@${participant.userId}> — ${formatDuration(participant.totalMs)} (${percentLabel})${badgeText}`;
  });

  const attendanceChunks = chunkLinesByLength(attendanceLines);
  const leaderboardLines = leaders.map((participant, index) => {
    const percentLabel = formatPercentageValue(participant.attendanceFraction);
    return `${index + 1}. <@${participant.userId}> — ${formatDuration(participant.totalMs)} (${percentLabel})`;
  });

  const firstArrivalLines = firstArrivals.map((participant) => {
    const offsetLabel = formatJoinOffsetBadge(participant.joinOffset);
    const displayLabel = offsetLabel || "on-time";
    return `<@${participant.userId}> ${displayLabel}`;
  });

  const badgeLegend = [
    `[full]=≥${formatPercentageValue(VOICE_SESSION_FULL_ATTENDANCE_THRESHOLD)} of session`,
    `[steady]=≥${formatPercentageValue(VOICE_SESSION_ON_TIME_THRESHOLD)} of session`,
    `[overtime]=beyond ${formatDuration(baseDurationMs)}`,
    `[on-time]/[+Xm]=arrival offset`
  ].join(" • ");

  const summaryEmbed = new EmbedBuilder()
    .setColor(0x2563eb)
    .setTitle("Voice Session Summary")
    .setDescription(`<#${session.channelId}> • ${session.channelName}`)
    .addFields(
      {
        name: "Duration",
        value: `${formatDuration(durationMs)} (${formatDateTime(session.startedAt)} → ${formatDateTime(now)})`,
        inline: false
      },
      {
        name: "Attendance",
        value: `${participantCount} unique • avg concurrent ${averageConcurrentLabel} • peak ${peakConcurrent}`,
        inline: false
      },
      {
        name: "Consistency",
        value: `${fullAttendanceCount} full (${formatPercentageValue(VOICE_SESSION_FULL_ATTENDANCE_THRESHOLD)}+) • ${steadyAttendanceCount} steady (${formatPercentageValue(VOICE_SESSION_ON_TIME_THRESHOLD)}+)`,
        inline: false
      },
      {
        name: "Overtime",
        value: overtimeMs > 0
          ? `${formatDuration(overtimeMs)} beyond ${formatDuration(baseDurationMs)}`
          : `Within ${formatDuration(baseDurationMs)}`,
        inline: false
      }
    );

  if (leaderboardLines.length) {
    summaryEmbed.addFields({ name: "Top Presence", value: leaderboardLines.join("\n"), inline: false });
  }

  if (firstArrivalLines.length) {
    summaryEmbed.addFields({ name: "First In", value: firstArrivalLines.join("\n"), inline: false });
  }

  summaryEmbed.addFields({ name: "Badge Legend", value: badgeLegend, inline: false });

  const rosterEmbeds = attendanceChunks.map((chunk, index) => {
    const embed = new EmbedBuilder().setColor(0x1d4ed8).setDescription(chunk);
    const totalChunks = attendanceChunks.length;
    const title = totalChunks > 1 ? `Attendance Roster (${index + 1}/${totalChunks})` : "Attendance Roster";
    embed.setTitle(title);
    return embed;
  });

  const allowedMentions = {
    users: participants.map((participant) => participant.userId),
    roles: []
  };

  await postLogEntry(`Voice session report — <#${session.channelId}>`, {
    embeds: [summaryEmbed, ...rosterEmbeds],
    allowedMentions
  });

  voiceSessions.delete(channelId);
};

const extractTaskDueText = (text) => {
  const byMatch = text.match(/\bby\b\s+(.+)/i);
  if (!byMatch) {
    return { details: text.trim(), dueText: null };
  }

  const dueText = byMatch[1].trim();
  if (!dueText) {
    return { details: text.trim(), dueText: null };
  }

  const details = text.slice(0, byMatch.index).trim() || text.trim();
  return { details, dueText };
};

const fetchMessageToForward = async (message, options = {}) => {
  const {
    preferAnyPrevious = false,
    includeSameAuthorFallback = true
  } = options;

  if (message.reference?.messageId) {
    try {
      const referenced = await message.channel.messages.fetch(message.reference.messageId);
      if (!referenced.author?.bot) {
        return referenced;
      }
    } catch (error) {
      console.error("Failed to fetch referenced message", error);
    }
  }

  let previousMessages;
  try {
    previousMessages = await message.channel.messages.fetch({ limit: 15, before: message.id });
  } catch (error) {
    console.error("Failed to fetch previous messages", error);
    return null;
  }

  const previousNonBot = [...previousMessages.values()].find((m) => !m.author?.bot);
  if (preferAnyPrevious && previousNonBot) {
    return previousNonBot;
  }

  if (includeSameAuthorFallback) {
    const previousFromAuthor = [...previousMessages.values()].find(
      (m) => m.author?.bot === false && m.author?.id === message.author.id
    );
    if (previousFromAuthor) {
      return previousFromAuthor;
    }
  }

  return preferAnyPrevious ? previousNonBot || null : null;
};

const sendChannelPing = async (message, rawContent) => {
  const pingIndex = rawContent.toLowerCase().indexOf("ping");
  if (pingIndex === -1) {
    return false;
  }

  const afterPing = rawContent.slice(pingIndex + 4).trim();
  if (!afterPing) {
    await message.reply("I need a channel mention after ping—try `Luciver ping #channel`.");
    return true;
  }

  const channelIds = [];
  const channelMentionRegex = /<#(\d+)>/g;
  let mentionMatch;
  while ((mentionMatch = channelMentionRegex.exec(afterPing)) !== null) {
    const channelId = mentionMatch[1];
    if (!channelIds.includes(channelId)) {
      channelIds.push(channelId);
    }
  }

  if (!channelIds.length) {
    await message.reply("Tag at least one channel—try `Luciver ping #db-management-team`.");
    return true;
  }

  let extraNote = afterPing.replace(/<#\d+>/g, " ").replace(/\s{2,}/g, " ").trim();
  extraNote = extraNote.replace(/^(and|,)+\s+/i, "").trim();

  const directiveRegex = /(send|share|forward)?\s*(the\s*)?(above|previous|last)\s*(message|msg|image|photo|pic|picture)/i;
  const usePreviousAny = directiveRegex.test(extraNote);
  if (usePreviousAny) {
    extraNote = extraNote.replace(directiveRegex, "").trim();
  }

  const sourceMessage = await fetchMessageToForward(message, {
    preferAnyPrevious: usePreviousAny,
    includeSameAuthorFallback: !usePreviousAny
  });

  const sourceText = sourceMessage?.content?.trim() || null;
  const cleanedNote = extraNote || null;
  const attachmentUrls = sourceMessage?.attachments?.size
    ? [...sourceMessage.attachments.values()].map((attachment) => attachment.url)
    : [];

  if (!sourceText && !cleanedNote && attachmentUrls.length === 0) {
    await message.reply("I need something to share—reply to the message you want forwarded or add a note after the channel list.");
    return true;
  }

  const bodySegments = [];
  if (sourceText) {
    bodySegments.push(sourceText);
  }
  if (attachmentUrls.length) {
    bodySegments.push("Attachments:\n" + attachmentUrls.join("\n"));
  }
  if (cleanedNote) {
    bodySegments.push(`Note from ${message.author}: ${cleanedNote}`);
  }

  const embed = new EmbedBuilder()
    .setAuthor({
      name: message.author.tag,
      iconURL: message.author.displayAvatarURL()
    })
    .setDescription(bodySegments.join("\n\n"))
    .setFooter({ text: `Forwarded from #${message.channel?.name || "unknown"}` })
    .setTimestamp(new Date());

  if (!embed.data.description) {
    embed.setDescription(`[No text content—attachments only]`);
  }

  const successes = [];
  const failures = [];

  for (const channelId of channelIds) {
    let targetChannel = null;
    try {
      targetChannel = await message.guild?.channels?.fetch(channelId);
    } catch (error) {
      console.error(`Failed to fetch channel ${channelId}`, error);
    }

    if (!targetChannel?.isTextBased()) {
      failures.push(channelId);
      continue;
    }

    try {
      const embedCopy = EmbedBuilder.from(embed);
      await targetChannel.send({
        content: `Forwarded via ${message.author}`,
        embeds: [embedCopy],
        allowedMentions: { parse: [] }
      });
      successes.push(targetChannel);
    } catch (error) {
      console.error(`Failed to forward message to ${channelId}`, error);
      failures.push(channelId);
    }
  }

  if (!successes.length) {
    await message.reply("I couldn't forward that—double-check my access to the mentioned channels.");
    return true;
  }

  const successMentions = successes.map((channel) => `<#${channel.id}>`).join(" ");
  let replyText = `Done. Shared your message with ${successMentions}.`;
  if (failures.length) {
    replyText += " Some channels could not be reached.";
  }

  await message.reply(replyText);
  return true;
};

const handleModeratorBulkDelete = async (message, rawContent) => {
  const normalized = rawContent.toLowerCase();
  const countMatch = rawContent.match(/\b(?:delete|remove)\s+(?:the\s+)?(?:above\s+)?(\d{1,3})\s+(?:msgs?|messages?)\b/i);
  const sweepMatch = /\b(?:delete|remove)\s+all\s+(?:the\s+)?above\s+(?:msgs?|messages?)(?:\s+in\s+this\s+channel)?\b/i.test(normalized)
    ? true
    : false;
  const rangeMatch = rawContent.match(/\b(?:delete|remove)\s+all\s+messages\s+from\s+(today|yesterday)\s+(?:up\s*to|until)\s+([0-9: ]+(?:am|pm)?)\b/i);
  const dateMatch = rawContent.match(
    /\b(?:delete|remove)\s+all\s+messages\s+from\s+(\d{1,2})(?:st|nd|rd|th)?\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)(?:\s+(\d{4}))?(?:\s+(?:up\s*to|until)\s+([0-9: ]+(?:am|pm)?))?\b/i
  );

  if (!countMatch && !sweepMatch && !rangeMatch && !dateMatch) {
    return false;
  }

  if (!message.inGuild()) {
    await message.reply("I can only clear messages inside a server channel.");
    return true;
  }

  if (!message.channel?.isTextBased()) {
    await message.reply("That channel doesn't support bulk message removal.");
    return true;
  }

  if (!hasModeratorPrivileges(message)) {
    await message.reply("Only moderators can ask me to remove messages.");
    return true;
  }

  const chunkIdsAndDelete = async (targets, context) => {
    const targetIds = targets.map((msg) => msg.id);
    let removedTotal = 0;
    let encounteredError = null;

    for (let index = 0; index < targetIds.length; index += 100) {
      const slice = targetIds.slice(index, index + 100);
      try {
        const deleted = await message.channel.bulkDelete(slice, true);
        removedTotal += deleted?.size ?? 0;
      } catch (error) {
        console.error("Failed to bulk delete messages", error);
        encounteredError = error;
        break;
      }
    }

    const skipped = targets.length - removedTotal;

    await postLogEntry(
      [
        "Moderator cleanup executed",
        `• Channel: <#${message.channel.id}>`,
        `• Requested by: <@${message.author.id}>`,
        `• Mode: ${context}`,
        `• Attempted: ${targets.length}`,
        `• Removed: ${removedTotal}`,
        skipped ? `• Skipped: ${skipped}` : null,
        `• Timestamp: ${formatDateTime(Date.now())}`
      ].filter(Boolean).join("\n"),
      { allowedMentions: { users: [message.author.id], roles: [] } }
    );

    return { removedTotal, skipped, encounteredError };
  };

  const fetchMessages = async (options) => {
    const targets = [];
    let beforeId = message.id;
    let keepFetching = true;
    const nowMs = Date.now();

    while (keepFetching && targets.length < options.max) {
      let batch;
      try {
        batch = await message.channel.messages.fetch({ limit: 100, before: beforeId });
      } catch (error) {
        console.error("Failed to fetch messages for moderator sweep", error);
        throw new Error("fetch-failed");
      }

      if (!batch?.size) {
        break;
      }

      const ordered = [...batch.values()];
      let earliest = null;

      for (const msg of ordered) {
        earliest = msg;

        if (nowMs - msg.createdTimestamp > DISCORD_BULK_DELETE_WINDOW_MS) {
          continue;
        }

        if (msg.pinned) {
          continue;
        }

        if (options.filter && !options.filter(msg)) {
          continue;
        }

        targets.push(msg);

        if (targets.length === options.max) {
          keepFetching = false;
          break;
        }
      }

      if (!earliest) {
        break;
      }

      beforeId = earliest.id;

      if (options.stopCondition && options.stopCondition(earliest)) {
        break;
      }
    }

    return targets;
  };

  if (rangeMatch) {
    const [, dayTokenRaw, timeTokenRaw] = rangeMatch;
    const dayToken = dayTokenRaw.toLowerCase();
    const nowZoned = DateTime.now().setZone(TARGET_TIMEZONE);
    let startBoundary = nowZoned.startOf("day");

    if (dayToken === "yesterday") {
      startBoundary = startBoundary.minus({ days: 1 });
    }

    const timeMatch = timeTokenRaw.trim();
    const timePattern = /^(\d{1,2})(?::(\d{2}))?\s*(am|pm)?$/i;
    const parsedTime = timePattern.exec(timeMatch);

    if (!parsedTime) {
      await message.reply("I couldn't parse that time - try something like 10am or 14:30.");
      return true;
    }

    let hour = Number(parsedTime[1]);
    const minute = parsedTime[2] ? Number(parsedTime[2]) : 0;
    const suffix = parsedTime[3]?.toLowerCase() ?? null;

    if (!Number.isFinite(hour) || hour > 23 || minute > 59) {
      await message.reply("That time doesn't look right - double-check the hour and minutes.");
      return true;
    }

    if (suffix) {
      if (hour === 12) {
        hour = suffix === "am" ? 0 : 12;
      } else if (suffix === "pm") {
        hour += 12;
      }
    }

    const endBoundary = startBoundary.set({ hour, minute, second: 0, millisecond: 0 });
    const cappedEnd = endBoundary > nowZoned ? nowZoned : endBoundary;

    if (cappedEnd <= startBoundary) {
      await message.reply("The time range you gave is empty - pick a later time.");
      return true;
    }

    const startMillis = startBoundary.toMillis();
    const endMillis = cappedEnd.toMillis();
    const cutoffMs = Date.now() - DISCORD_BULK_DELETE_WINDOW_MS;
    const effectiveStartMs = Math.max(startMillis, cutoffMs);

    if (effectiveStartMs > endMillis) {
      await message.reply("Those messages are older than Discord's 14-day bulk delete limit.");
      return true;
    }

    let targets;
    try {
      targets = await fetchMessages({
        max: MOD_DELETE_MAX_RANGE_MESSAGES,
        filter: (msg) => msg.createdTimestamp >= effectiveStartMs && msg.createdTimestamp <= endMillis,
        stopCondition: (earliest) => earliest.createdTimestamp < effectiveStartMs
      });
    } catch (error) {
      if (error.message === "fetch-failed") {
        await message.reply("I couldn't review the previous messages - try again in a moment.");
        return true;
      }
      throw error;
    }

    if (!targets.length) {
      await message.reply("I didn't find any messages in that timeframe to remove.");
      return true;
    }

    const { removedTotal, skipped, encounteredError } = await chunkIdsAndDelete(targets, `range (${dayToken} up to ${timeTokenRaw.trim()})`);

    if (encounteredError) {
      await message.reply("I hit a snag while clearing that range - some messages might remain.");
      return true;
    }

    const summaryParts = [`Removed ${removedTotal} message${removedTotal === 1 ? "" : "s"}`];
    if (skipped) {
      summaryParts.push(`${skipped} couldn't be removed (likely older than 14 days).`);
    }
    await message.reply(summaryParts.join(". "));
    return true;
  }

  if (dateMatch) {
    const [, dayRaw, monthRaw, yearRaw, timeTokenRaw] = dateMatch;
    const nowZoned = DateTime.now().setZone(TARGET_TIMEZONE);

    const day = Number(dayRaw);
    if (!Number.isFinite(day) || day < 1 || day > 31) {
      await message.reply("I couldn't understand that date - double-check the day number.");
      return true;
    }

    const monthBase = monthRaw.toLowerCase();
    const monthKey = monthBase.startsWith("sept") ? "sept" : monthBase.slice(0, 3);
    const monthNumber = MONTH_LOOKUP[monthKey];
    if (!monthNumber) {
      await message.reply("I couldn't understand that month - try spelling it like 'Dec' or 'December'.");
      return true;
    }

    let year = yearRaw ? Number(yearRaw) : nowZoned.year;
    if (!Number.isFinite(year) || year < 1970) {
      await message.reply("That year doesn't look right.");
      return true;
    }

    let startBoundary = DateTime.fromObject(
      { year, month: monthNumber, day },
      { zone: TARGET_TIMEZONE }
    );

    if (!startBoundary.isValid) {
      await message.reply("That date doesn't exist - double-check the day and month.");
      return true;
    }

    startBoundary = startBoundary.startOf("day");

    if (!yearRaw && startBoundary > nowZoned) {
      startBoundary = startBoundary.minus({ years: 1 });
    }

    let endBoundary;
    if (timeTokenRaw) {
      const trimmedTime = timeTokenRaw.trim();
      const timePattern = /^(\d{1,2})(?::(\d{2}))?\s*(am|pm)?$/i;
      const parsedTime = timePattern.exec(trimmedTime);

      if (!parsedTime) {
        await message.reply("I couldn't parse that time - try something like 10am or 14:30.");
        return true;
      }

      let hour = Number(parsedTime[1]);
      const minute = parsedTime[2] ? Number(parsedTime[2]) : 0;
      const suffix = parsedTime[3]?.toLowerCase() ?? null;

      if (!Number.isFinite(hour) || hour > 23 || minute > 59) {
        await message.reply("That time doesn't look right - double-check the hour and minutes.");
        return true;
      }

      if (suffix) {
        if (hour === 12) {
          hour = suffix === "am" ? 0 : 12;
        } else if (suffix === "pm") {
          hour += 12;
        }
      }

      endBoundary = startBoundary
        .set({ hour, minute, second: 0, millisecond: 0 })
        .plus({ minutes: 1 })
        .minus({ milliseconds: 1 });
    } else {
      endBoundary = startBoundary.plus({ days: 1 }).minus({ milliseconds: 1 });
    }

    const cappedEnd = endBoundary > nowZoned ? nowZoned : endBoundary;

    if (cappedEnd <= startBoundary) {
      await message.reply("That window doesn't include any time that has already happened.");
      return true;
    }

    const startMillis = startBoundary.toMillis();
    const endMillis = cappedEnd.toMillis();
    const cutoffMs = Date.now() - DISCORD_BULK_DELETE_WINDOW_MS;
    const effectiveStartMs = Math.max(startMillis, cutoffMs);

    if (effectiveStartMs > endMillis) {
      await message.reply("Those messages are older than Discord's 14-day bulk delete limit.");
      return true;
    }

    let targets;
    try {
      targets = await fetchMessages({
        max: MOD_DELETE_MAX_RANGE_MESSAGES,
        filter: (msg) => msg.createdTimestamp >= effectiveStartMs && msg.createdTimestamp <= endMillis,
        stopCondition: (earliest) => earliest.createdTimestamp < effectiveStartMs
      });
    } catch (error) {
      if (error.message === "fetch-failed") {
        await message.reply("I couldn't review the previous messages - try again in a moment.");
        return true;
      }
      throw error;
    }

    if (!targets.length) {
      await message.reply("I didn't find any messages in that date range to remove.");
      return true;
    }

    const labelDate = startBoundary.toFormat("dd LLL yyyy");
    const labelSuffix = timeTokenRaw ? ` until ${timeTokenRaw.trim()}` : "";

    const { removedTotal, skipped, encounteredError } = await chunkIdsAndDelete(
      targets,
      `date (${labelDate}${labelSuffix})`
    );

    if (encounteredError) {
      await message.reply("I hit a snag while clearing that date range - some messages might remain.");
      return true;
    }

    const summaryParts = [`Removed ${removedTotal} message${removedTotal === 1 ? "" : "s"}`];
    if (skipped) {
      summaryParts.push(`${skipped} couldn't be removed (likely older than 14 days).`);
    }
    await message.reply(summaryParts.join(". "));
    return true;
  }

  if (sweepMatch) {
    let targets;
    try {
      targets = await fetchMessages({
        max: MOD_DELETE_MAX_RANGE_MESSAGES,
        filter: () => true,
        stopCondition: (earliest) => Date.now() - earliest.createdTimestamp > DISCORD_BULK_DELETE_WINDOW_MS
      });
    } catch (error) {
      if (error.message === "fetch-failed") {
        await message.reply("I couldn't review the previous messages - try again in a moment.");
        return true;
      }
      throw error;
    }

    if (!targets.length) {
      await message.reply("I didn't find any removable messages above this one.");
      return true;
    }

    const { removedTotal, skipped, encounteredError } = await chunkIdsAndDelete(targets, "sweep above");

    if (encounteredError) {
      await message.reply("I ran into an issue while clearing those messages - some might still remain.");
      return true;
    }

    const summaryParts = [`Removed ${removedTotal} message${removedTotal === 1 ? "" : "s"}`];
    if (skipped) {
      summaryParts.push(`${skipped} couldn't be removed (likely older than 14 days).`);
    }
    await message.reply(summaryParts.join(". "));
    return true;
  }

  // Count-based branch
  const requestedCount = Number(countMatch[1]);
  if (!Number.isFinite(requestedCount) || requestedCount <= 0) {
    await message.reply("Tell me how many messages to remove - use a positive number.");
    return true;
  }

  if (requestedCount > MOD_DELETE_MAX_MESSAGES) {
    await message.reply(`I can remove up to ${MOD_DELETE_MAX_MESSAGES} recent messages at once.`);
    return true;
  }

  let fetched;
  try {
    fetched = await message.channel.messages.fetch({
      limit: Math.min(requestedCount + 5, MOD_DELETE_MAX_MESSAGES + 5),
      before: message.id
    });
  } catch (error) {
    console.error("Failed to fetch messages for moderator delete", error);
    await message.reply("I couldn't review the previous messages - try again in a moment.");
    return true;
  }

  if (!fetched?.size) {
    await message.reply("I couldn't find any messages above yours to remove.");
    return true;
  }

  const targets = [];
  for (const msg of fetched.values()) {
    if (msg.pinned) {
      continue;
    }
    targets.push(msg);
    if (targets.length === requestedCount) {
      break;
    }
  }

  if (!targets.length) {
    await message.reply("Everything above is pinned or already gone - I didn't remove anything.");
    return true;
  }

  const { removedTotal, skipped, encounteredError } = await chunkIdsAndDelete(targets, `count (${requestedCount})`);

  if (encounteredError) {
    await message.reply("I couldn't remove all of those messages. Some might remain - most likely they're older than 14 days.");
    return true;
  }

  const remaining = Math.max(0, requestedCount - removedTotal);
  const summary = remaining > 0
    ? `Removed ${removedTotal} message${removedTotal === 1 ? "" : "s"}. ${remaining} couldn't be removed (likely too old).`
    : `Removed ${removedTotal} message${removedTotal === 1 ? "" : "s"}.`;

  await message.reply(summary);
  return true;
};

const logTask = async (message, rawContent) => {
  const match = rawContent.match(/assign\s+<@!?(\d+)>\s+(?:to\s+)?(.+)/i);
  if (!match) {
    return false;
  }

  if (!hasModeratorPrivileges(message)) {
    await message.reply("Only moderators can assign tasks through me.");
    return true;
  }

  const [, assigneeId, details] = match;
  const cleanDetails = details.trim();
  if (!cleanDetails) {
    await message.reply("I need a task description after the teammate mention.");
    return true;
  }

  const { details: taskDescription, dueText } = extractTaskDueText(cleanDetails);

  taskBacklog.push({
    id: `${Date.now()}-${assigneeId}`,
    assigneeId,
    createdBy: message.author.id,
    details: taskDescription,
    channelId: message.channelId,
    createdAt: Date.now(),
    status: "open",
    lastNotifiedAt: 0,
    dueText: dueText || null
  });

  const confirmationParts = [
    `Task logged for <@${assigneeId}>: **${taskDescription}**.`,
    "I'll surface it in the weekly moderator digest."
  ];
  if (dueText) {
    confirmationParts.splice(1, 0, `Expected by: ${dueText}.`);
  }

  await message.reply(confirmationParts.join(" "));

  let dmStatus = "dm";
  const dmLines = [
    "Lumina assignment update",
    `• Task: **${taskDescription}**`,
    `• Assigned by: <@${message.author.id}>`,
    `• Channel: <#${message.channelId}>`
  ];
  if (dueText) {
    dmLines.splice(2, 0, `• Expected by: ${dueText}`);
  }

  try {
    const user = await client.users.fetch(assigneeId);
    await user.send(dmLines.join("\n"));
  } catch (error) {
    console.warn("Failed to DM assignee, falling back to channel", error);
    try {
      const originChannel = await client.channels.fetch(message.channelId);
      if (originChannel?.isTextBased()) {
        await originChannel.send({
          content: `<@${assigneeId}> ${dmLines.join("\n")}`,
          allowedMentions: { users: [assigneeId], roles: [] }
        });
        dmStatus = "channel";
      } else {
        dmStatus = "failed";
      }
    } catch (fallbackError) {
      console.error("Fallback delivery failed", fallbackError);
      dmStatus = "failed";
    }
  }

  await postLogEntry(
    [
      `Task assigned to <@${assigneeId}>`,
      `• Task: **${taskDescription}**`,
      dueText ? `• Expected by: ${dueText}` : null,
      `• Assigned by: <@${message.author.id}>`,
      `• Channel: <#${message.channelId}>`,
      `• Recorded at: ${formatDateTime(Date.now())}`,
      `• Delivery: ${dmStatus === "dm" ? "Direct Message" : dmStatus === "channel" ? "Channel Fallback" : "Failed"}`
    ].filter(Boolean).join("\n"),
    { allowedMentions: { users: [assigneeId, message.author.id], roles: [] } }
  );
  return true;
};

const DURATION_UNIT_LOOKUP = {
  minute: "minutes",
  minutes: "minutes",
  min: "minutes",
  mins: "minutes",
  hour: "hours",
  hours: "hours",
  hr: "hours",
  hrs: "hours",
  day: "days",
  days: "days"
};

const MONTH_LOOKUP = {
  jan: 1,
  feb: 2,
  mar: 3,
  apr: 4,
  may: 5,
  jun: 6,
  jul: 7,
  aug: 8,
  sep: 9,
  sept: 9,
  oct: 10,
  nov: 11,
  dec: 12
};

const sanitizeNote = (text) => text
  .replace(/\s{2,}/g, " ")
  .trim()
  .replace(/^[Tt]o\s+/, "")
  .replace(/[\s,.;:-]+$/, "");

const parseReminderSchedule = (text) => {
  const originalNoteRaw = typeof text === "string" ? text : "";
  const trimmedInput = originalNoteRaw.trim();

  let notePortion = trimmedInput;
  let schedulePortion = trimmedInput;
  let noteSeparated = false;

  const lastCommaIndex = trimmedInput.lastIndexOf(",");
  if (lastCommaIndex !== -1) {
    const before = trimmedInput.slice(0, lastCommaIndex).trim();
    const after = trimmedInput.slice(lastCommaIndex + 1).trim();
    if (after) {
      notePortion = before;
      schedulePortion = after;
      noteSeparated = true;
    }
  }

  let working = schedulePortion;
  let scheduled = null;
  let defaultedTime = false;
  const nowZoned = DateTime.now().setZone(TARGET_TIMEZONE);

  const durationMatch = working.match(/\bin\s*(\d+)\s*(minutes?|minute|mins?|hours?|hour|hrs?|days?|day)\b/i);
  if (durationMatch) {
    const amount = Number(durationMatch[1]);
    const unit = durationMatch[2].toLowerCase();
    const luxonUnit = DURATION_UNIT_LOOKUP[unit];
    if (!Number.isFinite(amount) || !luxonUnit) {
      return { error: "I couldn't understand the duration." };
    }
    scheduled = nowZoned.plus({ [luxonUnit]: amount });
    working = working.replace(durationMatch[0], "");
  }

  let explicitDate = null;

  const isoMatch = working.match(/\b(?:on\s+)?(\d{4}-\d{2}-\d{2})\b/i);
  if (isoMatch) {
    const isoDate = DateTime.fromISO(isoMatch[1], { zone: TARGET_TIMEZONE }).startOf("day");
    if (!isoDate.isValid) {
      return { error: "I couldn't parse that date." };
    }
    explicitDate = isoDate;
    working = working.replace(isoMatch[0], "");
  }

  if (!explicitDate) {
    const monthMatch = working.match(/\b(?:on\s+)?(\d{1,2})(?:st|nd|rd|th)?(?:\s|[-\/])?(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\b(?:\s*(\d{4}))?/i);
    if (monthMatch) {
      const day = Number(monthMatch[1]);
      const monthKey = monthMatch[2].slice(0, 4).toLowerCase();
      const normalizedKey = monthKey === "sept" ? "sept" : monthKey.slice(0, 3);
      const monthNumber = MONTH_LOOKUP[normalizedKey];
      const providedYear = monthMatch[3] ? Number(monthMatch[3]) : nowZoned.year;

      if (!monthNumber || !Number.isFinite(day) || day < 1 || day > 31) {
        return { error: "I couldn't parse that date." };
      }

      let candidate = DateTime.fromObject({ year: providedYear, month: monthNumber, day }, { zone: TARGET_TIMEZONE }).startOf("day");
      if (!candidate.isValid) {
        return { error: "I couldn't parse that date." };
      }

      if (!monthMatch[3] && candidate < nowZoned.startOf("day")) {
        candidate = candidate.plus({ years: 1 });
      }

      explicitDate = candidate;
      working = working.replace(monthMatch[0], "");
    }
  }

  const tomorrowMatch = working.match(/\b(?:tomorrow|tmrw|tmr)\b/i);
  let tomorrowFlag = false;
  if (tomorrowMatch) {
    tomorrowFlag = true;
    working = working.replace(tomorrowMatch[0], "");
  }

  const timeMatch = working.match(/(?:\bat\b\s*)?(\d{1,2}):(\d{2})(\s*[ap]m)?\b/i);
  const hourOnlyMatch = !timeMatch ? working.match(/(?:\bat\b\s*)?(\d{1,2})(\s*[ap]m)\b/i) : null;

  const applyTimeMatch = (match, minutesValue) => {
    const hoursRaw = Number(match[1]);
    const minutes = minutesValue;
    if (!Number.isFinite(hoursRaw) || hoursRaw > 23 || minutes > 59) {
      return { error: "That time doesn’t look right." };
    }

    let hours = hoursRaw;
    const suffixRaw = match[match.length - 1];
    const suffix = suffixRaw ? suffixRaw.trim().toLowerCase() : null;
    if (suffix) {
      if (hours === 12) {
        hours = suffix === "am" ? 0 : 12;
      } else if (suffix === "pm") {
        hours += 12;
      }
    }

    let baseDateTime = explicitDate || nowZoned;

    if (!baseDateTime.isValid) {
      return { error: "I couldn't parse that date." };
    }

    baseDateTime = baseDateTime.set({ hour: hours, minute: minutes, second: 0, millisecond: 0 });

    if (!explicitDate) {
      if (tomorrowFlag) {
        baseDateTime = baseDateTime.plus({ days: 1 });
      } else if (baseDateTime <= nowZoned) {
        baseDateTime = baseDateTime.plus({ days: 1 });
      }
    } else if (tomorrowFlag) {
      baseDateTime = baseDateTime.plus({ days: 1 });
    }

    scheduled = baseDateTime;
    working = working.replace(match[0], "");
    return null;
  };

  if (timeMatch) {
    const error = applyTimeMatch(timeMatch, Number(timeMatch[2]));
    if (error) {
      return error;
    }
  } else if (hourOnlyMatch) {
    const error = applyTimeMatch(hourOnlyMatch, 0);
    if (error) {
      return error;
    }
  }

  if (!scheduled && explicitDate) {
    scheduled = explicitDate.set({ hour: 9, minute: 0, second: 0, millisecond: 0 });
    defaultedTime = true;
  }

  if (!scheduled && tomorrowFlag) {
    scheduled = nowZoned.plus({ days: 1 }).set({ hour: 9, minute: 0, second: 0, millisecond: 0 });
    defaultedTime = true;
  }

  if (!scheduled) {
    return { error: "I need a time—try `in 15m`, `in 2 hours`, or `at 14:30`." };
  }

  if (noteSeparated) {
    const noteCheckSeparated = sanitizeNote(notePortion);
    if (!noteCheckSeparated) {
      return { error: "Tell me what to remind you about after the time." };
    }
  } else {
    const contentNote = sanitizeNote(working);
    if (!contentNote) {
      return { error: "Tell me what to remind you about after the time." };
    }

    const noteCheck = sanitizeNote(trimmedInput);
    if (!noteCheck) {
      return { error: "Tell me what to remind you about after the time." };
    }
  }

  if (scheduled.toMillis() <= nowZoned.toMillis()) {
    return { error: "That time already passed—give me something in the future." };
  }

  const note = noteSeparated ? notePortion : trimmedInput;

  return { note, dueAt: scheduled.toMillis(), defaultedTime };
};

const resolveReminderAudience = async (message, targetToken, matchedUserId, matchedRoleId) => {
  if (typeof targetToken !== "string") {
    return { error: "I couldn't figure out who to remind." };
  }

  const loweredToken = targetToken.toLowerCase();

  if (loweredToken === "me") {
    return {
      type: "user",
      id: message.author.id,
      guildId: message.guild?.id ?? null,
      displayLabel: `<@${message.author.id}>`,
      logLabel: `<@${message.author.id}>`
    };
  }

  if (loweredToken === "@everyone" || loweredToken === "everyone") {
    if (!message.guild) {
      return { error: "@everyone reminders only work inside a server." };
    }
    return {
      type: "everyone",
      id: message.guild.id,
      guildId: message.guild.id,
      displayLabel: "everyone",
      logLabel: "everyone"
    };
  }

  if (matchedUserId) {
    return {
      type: "user",
      id: matchedUserId,
      guildId: message.guild?.id ?? null,
      displayLabel: `<@${matchedUserId}>`,
      logLabel: `<@${matchedUserId}>`
    };
  }

  if (matchedRoleId) {
    if (!message.guild) {
      return { error: "I can only resolve roles inside a server." };
    }

    let role = message.guild.roles?.cache?.get(matchedRoleId) ?? null;
    if (!role) {
      try {
        role = await message.guild.roles.fetch(matchedRoleId);
      } catch (error) {
        console.warn("Failed to fetch role for reminder audience", error);
      }
    }

    if (!role) {
      return { error: "I couldn't find that role." };
    }

    const roleName = role.name || `role ${matchedRoleId}`;
    return {
      type: "role",
      id: role.id,
      guildId: message.guild.id,
      displayLabel: `${roleName} role`,
      logLabel: `${roleName} role`
    };
  }

  return { error: "I couldn't figure out who to remind." };
};

const ensureReminderAudience = (reminder) => {
  if (reminder.audience) {
    return reminder.audience;
  }

  if (reminder.targetId) {
    const fallback = {
      type: "user",
      id: reminder.targetId,
      guildId: reminder.guildId || null,
      displayLabel: `<@${reminder.targetId}>`,
      logLabel: `<@${reminder.targetId}>`
    };
    reminder.audience = fallback;
    return fallback;
  }

  return null;
};

const describeReminderAudience = (reminder, options = {}) => {
  const audience = ensureReminderAudience(reminder);
  if (!audience) {
    return "unknown recipients";
  }

  if (audience.type === "user") {
    const memberSnapshot = options.memberActivity?.get?.(audience.id);
    if (memberSnapshot?.tag) {
      return `@${memberSnapshot.tag}`;
    }
    return audience.displayLabel || `<@${audience.id}>`;
  }

  if (audience.type === "role") {
    return audience.displayLabel || "role recipients";
  }

  if (audience.type === "everyone") {
    return "everyone";
  }

  return audience.displayLabel || "group recipients";
};

const collectReminderRecipients = async (audience) => {
  if (!audience) {
    return [];
  }

  if (audience.type === "user") {
    try {
      const user = await client.users.fetch(audience.id);
      return user ? [user] : [];
    } catch (error) {
      console.warn("Failed to fetch user for reminder", error);
      return [];
    }
  }

  if (!audience.guildId) {
    return [];
  }

  let guild;
  try {
    guild = await client.guilds.fetch(audience.guildId);
  } catch (error) {
    console.warn("Unable to fetch guild for reminder audience", error);
    return [];
  }

  try {
    await guild.members.fetch();
  } catch (error) {
    console.warn("Unable to fetch full member list for reminder audience", error);
  }

  if (audience.type === "everyone") {
    const users = [];
    guild.members.cache.forEach((member) => {
      if (!member.user?.bot) {
        users.push(member.user);
      }
    });
    return users;
  }

  if (audience.type === "role") {
    const role = guild.roles.cache.get(audience.id);
    if (!role) {
      return [];
    }

    const users = [];
    role.members.forEach((member) => {
      if (!member.user?.bot) {
        users.push(member.user);
      }
    });
    return users;
  }

  return [];
};

const formatDateTime = (timestamp) => {
  const dt = DateTime.fromMillis(timestamp).setZone(TARGET_TIMEZONE);
  const label = dt.offsetNameShort || dt.offsetNameLong || TARGET_TIMEZONE;
  return `${dt.toLocaleString(DateTime.DATETIME_MED)} (${label})`;
};

const findModeratorRole = (guild) => {
  if (!guild) {
    return null;
  }

  const roles = guild.roles?.cache;
  if (!roles?.size) {
    return null;
  }

  return roles.find(
    (role) => typeof role.name === "string" && role.name.trim().toLowerCase().includes("moderator")
  ) || null;
};

const sanitizeReachOutContent = (message) => {
  if (!message?.content) {
    return "";
  }

  let content = message.content;
  const botId = client.user?.id;

  if (botId) {
    const leadingMentionPattern = new RegExp(`^\\s*<@!?${botId}>\\s*`, "i");
    content = content.replace(leadingMentionPattern, "");
    const genericMentionPattern = new RegExp(`<@!?${botId}>`, "gi");
    content = content.replace(genericMentionPattern, "");
  }

  content = content.replace(/^\s*luciver\b[\s,:-]*/i, "");

  return content.trim();
};

const buildReachOutEmbed = (message, detailsText, submittedAt) => {
  const description = detailsText ? detailsText.slice(0, 4000) : "No additional details provided.";
  const embed = new EmbedBuilder()
    .setColor(0xe17055)
    .setTitle("Reach-out Notice")
    .setDescription(description)
    .addFields(
      { name: "Member", value: `<@${message.author.id}>`, inline: true },
      { name: "Submitted", value: formatDateTime(submittedAt), inline: true },
      { name: "From Channel", value: `<#${message.channelId}>`, inline: true }
    )
    .setFooter({ text: "Luciver reach-out tracker" });

  if (message.guildId) {
    const jumpLink = `https://discord.com/channels/${message.guildId}/${message.channelId}/${message.id}`;
    embed.addFields({ name: "Message Link", value: `[Open update](${jumpLink})`, inline: false });
  }

  return embed;
};

const recordReachOutReport = (message, detailsText, submittedAt) => {
  const safeExcerpt = detailsText.length > 500 ? `${detailsText.slice(0, 497)}...` : detailsText;

  reachOutReports.push({
    authorId: message.author.id,
    channelId: message.channelId,
    messageId: message.id,
    submittedAt,
    excerpt: safeExcerpt
  });

  if (reachOutReports.length > REACH_OUT_MAX_RECORDS) {
    reachOutReports.shift();
  }
};

const handleReachOutExcuse = async (message) => {
  if (!message.inGuild()) {
    return false;
  }

  const channelName = message.channel?.name?.trim().toLowerCase();
  if (channelName !== REACH_OUT_CHANNEL_NAME) {
    return false;
  }

  const content = message.content?.toLowerCase() || "";
  const mentionsLuciver = message.mentions.has(client.user) || content.includes("luciver");
  if (!mentionsLuciver) {
    return false;
  }

  const submittedAt = Date.now();
  const detailsText = sanitizeReachOutContent(message);

  if (!detailsText) {
    await message.reply("I need a quick note after you tag me so I can brief the moderators.");
    return true;
  }

  const loweredDetails = detailsText.toLowerCase();
  if (/^(delete|remove)\b/.test(loweredDetails)) {
    return false;
  }

  recordReachOutReport(message, detailsText, submittedAt);

  const embed = buildReachOutEmbed(message, detailsText, submittedAt);
  const moderatorRole = findModeratorRole(message.guild);
  const allowedMentions = {
    users: [message.author.id],
    roles: moderatorRole ? [moderatorRole.id] : []
  };

  const mentionLabel = moderatorRole ? `<@&${moderatorRole.id}>` : "Moderators";
  const noticeHeadline = moderatorRole
    ? `${mentionLabel} new reach-out update.`
    : "Moderators, new reach-out update.";

  let notifiedModerators = false;
  let moderatorChannel = null;

  if (MODERATOR_CHANNEL_ID) {
    try {
      moderatorChannel = await client.channels.fetch(MODERATOR_CHANNEL_ID);
    } catch (error) {
      console.error("Failed to fetch moderator channel for reach-out notice", error);
    }

    if (moderatorChannel?.isTextBased()) {
      try {
        await moderatorChannel.send({
          content: noticeHeadline,
          embeds: [embed],
          allowedMentions
        });
        notifiedModerators = true;
      } catch (error) {
        console.error("Failed to post reach-out notice to moderator channel", error);
      }
    }
  }

  if (!notifiedModerators) {
    await message.reply(
      "I couldn't reach the moderators channel for this notice. Please double-check my configuration or let them know directly."
    );
    return true;
  }

  await message.reply("Thanks for looping me in. I've shared this update with the moderators!!");

  return true;
};

const dispatchDailyRoleReminder = async () => {
  const normalizedRoleName = DAILY_REMINDER_ROLE_NAME.trim().toLowerCase();
  const targetedRoles = [];
  const recipientMap = new Map();

  for (const guild of client.guilds.cache.values()) {
    let role = guild.roles.cache.find((entry) => typeof entry.name === "string" && entry.name.trim().toLowerCase() === normalizedRoleName) || null;

    if (!role) {
      try {
        const roles = await guild.roles.fetch();
        role = roles.find((entry) => typeof entry.name === "string" && entry.name.trim().toLowerCase() === normalizedRoleName) || null;
      } catch (error) {
        console.warn("Unable to fetch roles while preparing daily reminder", error);
      }
    }

    if (!role) {
      continue;
    }

    targetedRoles.push(`${role.name} (${guild.name})`);

    try {
      await guild.members.fetch();
    } catch (error) {
      console.warn("Unable to fetch members while preparing daily reminder", error);
    }

    role.members.forEach((member) => {
      if (!member.user?.bot && !recipientMap.has(member.id)) {
        recipientMap.set(member.id, member.user);
      }
    });
  }

  const recipients = [...recipientMap.values()];
  let successes = 0;
  const failures = [];

  for (const user of recipients) {
    try {
      await user.send(DAILY_REMINDER_MESSAGE);
      successes += 1;
    } catch (error) {
      console.warn("Failed to DM daily reminder recipient", error);
      failures.push(user.id);
    }
  }

  const attempts = recipients.length;
  const nowLabel = formatDateTime(Date.now());
  const logLines = [
    "Daily reminder has been sent",
    targetedRoles.length
      ? `• Target role: ${targetedRoles.join(", ")}`
      : `• Target role: "${DAILY_REMINDER_ROLE_NAME}" not found`,
    `• Scheduled at: ${nowLabel}`,
    `• Recipients reached: ${successes}/${attempts}`,
    failures.length ? `• DM failures: ${failures.length}` : null
  ].filter(Boolean).join("\n");

  await postLogEntry(logLines, { allowedMentions: { users: [], roles: [] } });
};

const computeNextDailyRoleReminder = () => {
  const now = DateTime.now().setZone(TARGET_TIMEZONE);
  let next = now.set({
    hour: DAILY_REMINDER_HOUR,
    minute: DAILY_REMINDER_MINUTE,
    second: 0,
    millisecond: 0
  });

  if (next <= now) {
    next = next.plus({ days: 1 });
  }

  return next;
};

const scheduleDailyRoleReminder = () => {
  if (dailyRoleReminderTimeout) {
    clearTimeout(dailyRoleReminderTimeout);
    dailyRoleReminderTimeout = null;
  }

  const next = computeNextDailyRoleReminder();
  const delay = Math.max(0, next.toMillis() - Date.now());

  dailyRoleReminderTimeout = setTimeout(async () => {
    dailyRoleReminderTimeout = null;
    try {
      await dispatchDailyRoleReminder();
    } catch (error) {
      console.error("Daily role reminder dispatch failed", error);
    } finally {
      scheduleDailyRoleReminder();
    }
  }, delay);
};

const logReminder = async (message, rawContent) => {
  const match = rawContent.match(/remind\s+(me|@?everyone|<@!?(\d+)>|<@&(\d+)>)(?:\s+to)?\s+(.+)/i);
  if (!match) {
    return false;
  }

  const [, targetToken, matchedUserId, matchedRoleId, remainder] = match;
  const cleanReminder = remainder.trim();
  if (!cleanReminder) {
    await message.reply("Give me what to remind you about—try `Luciver remind me to review PR in 30m`." );
    return true;
  }

  const audience = await resolveReminderAudience(message, targetToken, matchedUserId, matchedRoleId);
  if (audience.error) {
    await message.reply(audience.error);
    return true;
  }

  const schedule = parseReminderSchedule(cleanReminder);
  if (schedule.error) {
    await message.reply(`${schedule.error} Examples: 
  - Luciver remind me to stretch in 20m
  - Luciver remind @Nova ship notes at 16:30
  - Luciver remind me update roadmap on 2025-12-15 at 09:00`);
    return true;
  }

  reminderQueue.push({
    id: `${Date.now()}-${audience.id}`,
    audience,
    targetId: audience.type === "user" ? audience.id : null,
    guildId: audience.guildId || null,
    createdBy: message.author.id,
    note: schedule.note,
    channelId: message.channelId,
    createdAt: Date.now(),
    dueAt: schedule.dueAt,
    sentAt: null,
    defaultedTime: schedule.defaultedTime || false
  });

  const defaultTimeHint = schedule.defaultedTime
    ? " I set that to 09:00 in the configured timezone—add a time if you need something different."
    : "";

  const audienceNoun = audience.type === "user" ? "them" : audience.type === "everyone" ? "everyone" : "that group";
  await message.reply(
    `Reminder saved for ${audience.displayLabel}: ${schedule.note}. I'll DM ${audienceNoun} around ${formatDateTime(schedule.dueAt)}.${defaultTimeHint}`
  );

  const logMentionUsers = new Set([message.author.id]);
  if (audience.type === "user") {
    logMentionUsers.add(audience.id);
  }

  await postLogEntry(
    [
      `Reminder scheduled for ${audience.logLabel}`,
      `• Note: ${schedule.note}`,
      `• Due: ${formatDateTime(schedule.dueAt)}`,
      schedule.defaultedTime ? "• Time detail: Defaulted to 09:00 (no explicit time provided)" : null,
      `• Requested by: <@${message.author.id}>`,
      `• Channel: <#${message.channelId}>`
    ].join("\n"),
    { allowedMentions: { users: [...logMentionUsers], roles: [] } }
  );
  return true;
};

const shareStats = async (message) => {
  if (!channelActivity.size) {
    await message.reply("I'm still watching the room—no stats yet.");
    return true;
  }

  const now = Date.now();
  const dayMs = 24 * 60 * 60 * 1000;
  const quietThresholdMs = 3 * dayMs;

  const channelEntries = [...channelActivity.entries()];
  const memberEntries = [...memberActivity.entries()];

  const totalMessages = channelEntries.reduce((acc, [, entry]) => acc + entry.count, 0);
  const recentChannels = channelEntries.filter(([, entry]) => now - entry.updatedAt <= dayMs).length;
  const avgMessagesPerChannel = channelEntries.length
    ? Math.round((totalMessages / channelEntries.length) * 10) / 10
    : 0;
  const uniqueContributors = memberEntries.length;

  const summaryLines = [
    `Total tracked messages: **${totalMessages}**`,
    `Active contributors: **${uniqueContributors}**`,
    `Channels active (24h): **${recentChannels}/${channelEntries.length}**`,
    `Avg msgs per channel: **${avgMessagesPerChannel}**`,
    `Snapshot: ${formatDateTime(now)}`
  ].join("\n");

  const sortedChannels = channelEntries
    .slice()
    .sort((a, b) => b[1].count - a[1].count)
    .slice(0, 5)
    .map(([channelId, entry], index) => {
      const label = entry.name ? `#${entry.name}` : `<#${channelId}>`;
      return `${index + 1}. ${label}: ${entry.count} msgs (last spark ${relativeTime(entry.updatedAt)})`;
    })
    .join("\n") || "Not enough channel activity captured yet.";

  const channelNameFor = (channelId) => {
    const stored = channelActivity.get(channelId);
    if (stored?.name) {
      return `#${stored.name}`;
    }
    const cached = message.guild?.channels?.cache?.get(channelId)?.name;
    return cached ? `#${cached}` : "unknown channel";
  };

  const sortedMembers = memberEntries
    .slice()
    .sort((a, b) => b[1].count - a[1].count)
    .slice(0, 5)
    .map(([memberId, data], index) => {
      const displayTag = data.tag ? `@${data.tag}` : `User ${memberId}`;
      const channelLabel = data.lastChannelId ? channelNameFor(data.lastChannelId) : "unknown channel";
      return `${index + 1}. ${displayTag}: ${data.count} msgs (last seen ${relativeTime(data.updatedAt)} in ${channelLabel})`;
    })
    .join("\n") || "Not enough member activity captured yet.";

  const quietChannels = channelEntries
    .filter(([, entry]) => now - entry.updatedAt > quietThresholdMs)
    .sort((a, b) => a[1].updatedAt - b[1].updatedAt)
    .slice(0, 3)
    .map(([channelId, entry]) => {
      const label = entry.name ? `#${entry.name}` : `<#${channelId}>`;
      return `${label} – last spark ${relativeTime(entry.updatedAt)}`;
    })
    .join("\n");

  const openTasks = taskBacklog.filter((task) => task.status === "open");
  const pendingReminders = reminderQueue.filter((reminder) => !reminder.sentAt);
  const dueSoonReminders = pendingReminders.filter((reminder) => reminder.dueAt - now <= dayMs);
  const nextReminder = pendingReminders
    .slice()
    .sort((a, b) => a.dueAt - b.dueAt)[0];

  const nextReminderLabel = nextReminder
    ? `${formatDateTime(nextReminder.dueAt)} for ${describeReminderAudience(nextReminder, { memberActivity })}`
    : "None queued";

  const lastReachOut = reachOutReports[reachOutReports.length - 1];
  const reachOutSummary = reachOutReports.length
    ? `Reach-out notices logged: **${reachOutReports.length}** (latest ${relativeTime(lastReachOut.submittedAt)})`
    : "Reach-out notices logged: **0**";

  const operationsLines = [
    `Open assignments: **${openTasks.length}**`,
    `Pending reminders (<24h): **${dueSoonReminders.length}**`,
    `Next reminder: ${nextReminderLabel}`,
    reachOutSummary
  ].join("\n");

  const embed = new EmbedBuilder()
    .setTitle("Server Pulse")
    .setColor(0x00b894)
    .setDescription(summaryLines)
    .setFooter({ text: "Need more details? We'll wire a dashboard soon." })
    .addFields({ name: "Top Channels", value: sortedChannels, inline: false })
    .addFields({ name: "Top Contributors", value: sortedMembers, inline: false })
    .addFields({
      name: "Quiet Spots",
      value: quietChannels || "All tracked channels have recent activity.",
      inline: false
    })
    .addFields({ name: "Operations Snapshot", value: operationsLines, inline: false });

  const roleStats = await fetchMemberRoleCounts(message.guild);
  const roleSnapshot = formatRoleSnapshot(roleStats);
  if (roleSnapshot) {
    embed.addFields({ name: "Role Snapshot", value: roleSnapshot, inline: false });
  }

  await message.reply({ embeds: [embed] });
  return true;
};

const handleLuciverCue = async (message, rawContent) => {
  const normalized = rawContent.toLowerCase();
  const nameMentioned = normalized.includes("luciver") || message.mentions.has(client.user);
  if (!nameMentioned) {
    return false;
  }

  if (/\bhelp\b/.test(normalized)) {
    await message.reply({ embeds: [buildHelpEmbed()] });
    return true;
  }

  if (/\bping\b/.test(normalized) && message.guild) {
    const handled = await sendChannelPing(message, rawContent);
    if (handled) {
      return true;
    }
  }

  if (/\b(delete|remove)\b/.test(normalized)) {
    const handled = await handleModeratorBulkDelete(message, rawContent);
    if (handled) {
      return true;
    }
  }

  if (/\breminders\b/.test(normalized)) {
    const handled = await handleReminderAdminCommand(message, rawContent);
    if (handled) {
      return true;
    }
  }

  if (/\bassign\b/.test(normalized)) {
    const handled = await logTask(message, rawContent);
    if (handled) {
      return true;
    }
  }

  if (/\bremind\b/.test(normalized)) {
    const handled = await logReminder(message, rawContent);
    if (handled) {
      return true;
    }
  }

  if (/\b(stats|status|pulse)\b/.test(normalized)) {
    const handled = await shareStats(message);
    if (handled) {
      return true;
    }
  }

  return false;
};

const deliverReminder = async (reminder) => {
  const audience = ensureReminderAudience(reminder);
  const { note, channelId, createdBy, dueAt, defaultedTime } = reminder;
  const authorMention = `<@${createdBy}>`;
  const audienceLabel = audience?.displayLabel || "the target";
  const reminderLines = [
    `Reminder checkpoint for ${audienceLabel}`,
    `Note: **${note}**`,
    `Scheduled for: ${formatDateTime(dueAt)}`,
    `Requested by: ${authorMention}`
  ];
  const reminderText = reminderLines.join("\n");

  const recipients = await collectReminderRecipients(audience);
  const successes = [];
  const failures = [];

  for (const user of recipients) {
    try {
      await user.send(reminderText);
      successes.push(user.id);
    } catch (error) {
      console.warn("Failed to DM reminder recipient", error);
      failures.push(user.id);
    }
  }

  let fallbackUsed = null;
  if (!successes.length && audience?.type === "user" && channelId) {
    try {
      const channel = await client.channels.fetch(channelId);
      if (channel?.isTextBased()) {
        await channel.send({
          content: reminderText,
          allowedMentions: {
            users: [audience.id, createdBy].filter(Boolean),
            roles: []
          }
        });
        fallbackUsed = "channel";
      }
    } catch (error) {
      console.error("Failed to post reminder to original channel", error);
    }
  }

  const totalRecipients = recipients.length;
  let deliveryState = "failed";
  let deliveryLabel = totalRecipients ? "Direct Messages failed" : "No recipients";

  if (totalRecipients && successes.length === totalRecipients) {
    deliveryState = "dm";
    deliveryLabel = `Direct Messages (${successes.length})`;
  } else if (successes.length && totalRecipients) {
    deliveryState = "partial";
    deliveryLabel = `Partial delivery (${successes.length}/${totalRecipients})`;
  }

  if (fallbackUsed === "channel") {
    deliveryState = "channel";
    deliveryLabel = "Channel fallback";
  }

  const logLines = [
    `Reminder delivered to ${describeReminderAudience(reminder)}`,
    `• Note: ${note}`,
    `• Scheduled for: ${formatDateTime(dueAt)}`,
    defaultedTime ? "• Time detail: Defaulted to 09:00 (no explicit time provided)" : null,
    `• Delivery: ${deliveryLabel}`,
    totalRecipients ? `• Recipients reached: ${successes.length}/${totalRecipients}` : null,
    failures.length && totalRecipients ? `• DM failures: ${failures.length}` : null,
    fallbackUsed === "channel" ? "• Fallback: Posted in original channel" : null,
    `• Requested by: <@${createdBy}>`,
    `• Original channel: <#${channelId}>`,
    `• Sent at: ${formatDateTime(Date.now())}`
  ].filter(Boolean).join("\n");

  const logMentionUsers = new Set([createdBy]);
  if (audience?.type === "user") {
    logMentionUsers.add(audience.id);
  }

  await postLogEntry(logLines, {
    allowedMentions: {
      users: [...logMentionUsers],
      roles: []
    }
  });

  return deliveryState;
};

const collectPendingReminders = () =>
  reminderQueue
    .filter((entry) => !entry.sentAt)
    .sort((a, b) => a.dueAt - b.dueAt);

const formatReminderAdminLine = (reminder, index) => {
  const audienceLabel = describeReminderAudience(reminder, { memberActivity });
  const requesterLabel = reminder.createdBy ? `<@${reminder.createdBy}>` : "unknown requester";
  const createdAgo = reminder.createdAt ? relativeTime(reminder.createdAt) : "unknown";
  const defaultFlag = reminder.defaultedTime ? " (default 09:00)" : "";
  return `[#${index + 1}] ${audienceLabel} → ${reminder.note} (Due ${formatDateTime(reminder.dueAt)}${defaultFlag}, requested by ${requesterLabel}, queued ${createdAgo}, id ${reminder.id})`;
};

const handleReminderAdminCommand = async (message, rawContent) => {
  if (!hasModeratorPrivileges(message)) {
    await message.reply("Reminder admin commands are moderator-only.");
    return true;
  }

  const deleteMatch = rawContent.match(/reminders?\s+delete\s+([^\s]+)/i);
  if (!deleteMatch) {
    const pending = collectPendingReminders();
    if (!pending.length) {
      await message.reply("No pending reminders right now.");
      return true;
    }

    const limit = 10;
    const lines = pending.slice(0, limit).map((reminder, index) => formatReminderAdminLine(reminder, index));
    if (pending.length > limit) {
      lines.push(`…and ${pending.length - limit} more pending reminders.`);
    }

    await message.reply(lines.join("\n"));
    return true;
  }

  const tokenRaw = deleteMatch[1].trim();
  const pending = collectPendingReminders();
  if (!pending.length) {
    await message.reply("No pending reminders right now.");
    return true;
  }

  let target = null;
  const token = tokenRaw.startsWith("#") ? tokenRaw.slice(1) : tokenRaw;
  if (/^\d+$/.test(token)) {
    const index = Number(token) - 1;
    if (index >= 0 && index < pending.length) {
      target = pending[index];
    }
  }

  if (!target) {
    target = pending.find((entry) => entry.id === tokenRaw || entry.id === token);
  }

  if (!target) {
    await message.reply("I couldn't find a pending reminder matching that token. Use `Luciver reminders` to see the current list.");
    return true;
  }

  const originalIndex = reminderQueue.indexOf(target);
  if (originalIndex >= 0) {
    reminderQueue.splice(originalIndex, 1);
  }

  const audienceLabel = describeReminderAudience(target, { memberActivity });
  await message.reply(`Reminder cancelled: ${audienceLabel} → ${target.note} (ID ${target.id}).`);

  const logMentionUsers = new Set([message.author.id]);
  if (target.createdBy) {
    logMentionUsers.add(target.createdBy);
  }

  await postLogEntry(
    [
      "Reminder cancelled",
      `• Note: ${target.note}`,
      `• Due: ${formatDateTime(target.dueAt)}`,
      target.defaultedTime ? "• Time detail: Defaulted to 09:00 (no explicit time provided)" : null,
      `• Audience: ${audienceLabel}`,
      target.createdBy ? `• Originally requested by: <@${target.createdBy}>` : null,
      `• Cancelled by: <@${message.author.id}>`,
      `• Channel: <#${message.channelId}>`
    ].filter(Boolean).join("\n"),
    { allowedMentions: { users: [...logMentionUsers], roles: [] } }
  );

  return true;
};

const processDueReminders = async () => {
  if (!reminderQueue.length) {
    return;
  }

  const now = Date.now();
  const dueReminders = reminderQueue.filter((entry) => !entry.sentAt && entry.dueAt <= now);

  for (const reminder of dueReminders) {
    const status = await deliverReminder(reminder);
    reminder.sentAt = Date.now();
    reminder.delivery = status;
  }
};

const maybeSendTaskDigest = async () => {
  if (!MODERATOR_CHANNEL_ID) {
    return;
  }

  const openTasks = taskBacklog.filter((task) => task.status === "open");
  if (!openTasks.length) {
    return;
  }

  const now = DateTime.now().setZone(TARGET_TIMEZONE);
  if (now.weekday % 7 !== TASK_DIGEST_TARGET_DAY || now.hour !== TASK_DIGEST_TARGET_HOUR) {
    return;
  }

  if (Date.now() - lastTaskDigestSentAt < TASK_DIGEST_MIN_INTERVAL_MS) {
    return;
  }

  let moderatorChannel;
  try {
    moderatorChannel = await client.channels.fetch(MODERATOR_CHANNEL_ID);
  } catch (error) {
    console.error("Cannot fetch moderator channel", error);
    return;
  }

  if (!moderatorChannel?.isTextBased()) {
    return;
  }

  const grouped = openTasks.reduce((acc, task) => {
    const list = acc.get(task.assigneeId) || [];
    list.push(task);
    acc.set(task.assigneeId, list);
    return acc;
  }, new Map());

  const lines = [
    `Weekly task digest (${formatDateTime(Date.now())})`,
    "",
    ...[...grouped.entries()].map(([assigneeId, tasks]) => {
      const taskLines = tasks
        .map((task) => `  • **${task.details}** (assigned ${relativeTime(task.createdAt)})`)
        .join("\n");
      return `<@${assigneeId}>\n${taskLines}`;
    })
  ];

  try {
    const digestText = lines.join("\n");
    await moderatorChannel.send({ content: digestText, allowedMentions: { users: [...grouped.keys()], roles: [] } });
    await postLogEntry(
      [
        `Weekly task digest shared`,
        `• Posted to: ${moderatorChannel}`,
        `• Total assignees: ${grouped.size}`,
        `• Total tasks: ${openTasks.length}`,
        "",
        digestText
      ].join("\n"),
      { allowedMentions: { users: [], roles: [] } }
    );
    lastTaskDigestSentAt = Date.now();
  } catch (error) {
    console.error("Failed to send task digest", error);
  }
};

const maybeSendStatsReport = async () => {
  if (!LUCIVER_LOG_CHANNEL_ID || !channelActivity.size) {
    return;
  }

  const logChannel = await fetchLogChannel();
  if (!logChannel) {
    return;
  }

  const now = DateTime.now().setZone(TARGET_TIMEZONE);
  if (now.weekday % 7 !== STATS_REPORT_TARGET_DAY || now.hour !== STATS_REPORT_TARGET_HOUR) {
    return;
  }

  if (Date.now() - lastStatsReportSentAt < STATS_REPORT_MIN_INTERVAL_MS) {
    return;
  }

  const totalMessages = [...channelActivity.values()].reduce((acc, entry) => acc + entry.count, 0);
  const topChannels = [...channelActivity.values()]
    .sort((a, b) => b.count - a.count)
    .slice(0, 5)
    .map((entry, index) => `${index + 1}. #${entry.name}: ${entry.count} msgs (last spark ${relativeTime(entry.updatedAt)})`)
    .join("\n");

  const roleStats = await fetchMemberRoleCounts(logChannel.guild);
  const roleSnapshot = formatRoleSnapshot(roleStats);

  await postLogEntry(
    [
      `Weekend pulse (${formatDateTime(Date.now())})`,
      `• Total tracked messages: ${totalMessages}`,
      `• Channels watched: ${channelActivity.size}`,
      roleSnapshot ? `• Role snapshot: ${roleSnapshot}` : null,
      "",
      topChannels || "No channel activity captured yet."
    ].filter(Boolean).join("\n"),
    { allowedMentions: { users: [], roles: [] } }
  );

  lastStatsReportSentAt = Date.now();
};

const startSchedulers = () => {
  scheduleDailyRoleReminder();

  setInterval(() => {
    processDueReminders().catch((error) => console.error("Reminder sweep failed", error));
  }, REMINDER_CHECK_INTERVAL_MS);

  setInterval(() => {
    maybeSendTaskDigest().catch((error) => console.error("Task digest sweep failed", error));
  }, TASK_DIGEST_CHECK_INTERVAL_MS);

  setInterval(() => {
    maybeSendStatsReport().catch((error) => console.error("Stats report sweep failed", error));
  }, TASK_DIGEST_CHECK_INTERVAL_MS);
};

client.on("messageCreate", async (message) => {
  if (message.author?.bot) {
    return;
  }

  recordChannelActivity(message);

  const rawContent = message.content?.trim();
  if (!rawContent) {
    return;
  }

  const normalized = rawContent.toLowerCase();

  if (isGreetingMessage(message)) {
    await message.reply(greetingIntroResponse);
    return;
  }

  const nameMentioned = message.mentions.has(client.user) || normalized.includes("luciver");
  if (nameMentioned && isPlainNamePing(message)) {
    await message.reply(namePingResponse);
    return;
  }

  if (nameMentioned) {
    const handled = await handleLuciverCue(message, rawContent);
    if (handled) {
      return;
    }
  }

  if (await handleReachOutExcuse(message)) {
    return;
  }
});

client.on("voiceStateUpdate", async (oldState, newState) => {
  const oldChannel = oldState.channel;
  const newChannel = newState.channel;

  if (oldChannel?.id === newChannel?.id) {
    return;
  }

  if (oldChannel && isTrackedVoiceChannel(oldChannel)) {
    const session = voiceSessions.get(oldChannel.id);
    if (session) {
      markParticipantLeave(session, oldState);
      if (session.activeCount === 0) {
        await finalizeVoiceSession(oldChannel.id);
      }
    }
  }

  if (newChannel && isTrackedVoiceChannel(newChannel)) {
    const session = ensureVoiceSession(newChannel);
    markParticipantJoin(session, newState);
  }
});

const token = process.env.TOKEN;
if (!token) {
  console.error("Discord bot token is missing. Set TOKEN in your .env file.");
  process.exit(1);
}

client.login(token).catch((error) => {
  console.error("Failed to log in:", error);
});
