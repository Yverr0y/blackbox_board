#!/usr/bin/env node
/**
 * Performance test for forum synchronization
 * Simulates receiving multiple messages and measures response time
 */

const fs = require('fs');
const path = require('path');

const TEST_DIR = path.join(__dirname, 'test-data');

// Create test utilities
function generateEnvelopes(count) {
  const envelopes = [];

  for (let i = 0; i < count; i++) {
    if (i % 2 === 0) {
      // Topic create
      envelopes.push({
        p: 'mf1',
        t: 'topic_create',
        id: `test-t${i}`,
        topicId: `topic-${Math.floor(i/2)}`,
        title: `Test Topic ${i}`,
        body: `This is test topic ${i}`,
        ts: Date.now() + i * 1000,
        a: 'author1',
        n: 'Author One'
      });
    } else {
      // Post create
      envelopes.push({
        p: 'mf1',
        t: 'post_create',
        id: `test-p${i}`,
        topicId: `topic-${Math.floor(i/2)}`,
        postId: `post-${i}`,
        body: `This is test post ${i}`,
        ts: Date.now() + i * 1000,
        a: 'author2',
        n: 'Author Two'
      });
    }
  }

  return envelopes;
}

function formatDuration(ms) {
  if (ms < 1000) {
    return `${ms.toFixed(1)}ms`;
  }
  return `${(ms / 1000).toFixed(2)}s`;
}

function runTest(messageCount) {
  console.log(`\n📊 Performance Test: ${messageCount} messages\n`);

  const envelopes = generateEnvelopes(messageCount);

  // Simulate old approach: sync writes
  console.log('Testing OLD approach (synchronous writes)...');
  const startSync = Date.now();

  try {
    fs.mkdirSync(path.join(TEST_DIR, 'old'), { recursive: true });
    let forumState = {
      topics: [],
      posts: [],
      seenEnvelopeIds: []
    };

    for (const envelope of envelopes) {
      if (envelope.t === 'topic_create') {
        forumState.topics.push({
          topicId: envelope.topicId,
          title: envelope.title,
          bodyPreview: envelope.body.slice(0, 140),
          createdAt: new Date(envelope.ts).toISOString(),
          bumpedAt: new Date(envelope.ts).toISOString(),
        });
      } else {
        forumState.posts.push({
          postId: envelope.postId,
          topicId: envelope.topicId,
          body: envelope.body,
          createdAt: new Date(envelope.ts).toISOString(),
        });
      }
      forumState.seenEnvelopeIds.push(envelope.id);

      // OLD: Synchronous write for each message
      fs.writeFileSync(
        path.join(TEST_DIR, 'old', 'forum.json'),
        JSON.stringify(forumState, null, 2)
      );
    }
  } catch (err) {
    console.error('Error in old approach:', err.message);
  }

  const durationSync = Date.now() - startSync;
  console.log(`⏱️  OLD (sync writes): ${formatDuration(durationSync)}\n`);

  // Simulate new approach: async batched writes
  console.log('Testing NEW approach (async batched writes)...');
  const startAsync = Date.now();

  let writeScheduled = false;
  let writePending = false;
  let writeCount = 0;

  async function scheduleBatchedWrite(state) {
    writePending = true;

    if (writeScheduled) {
      return;
    }

    writeScheduled = true;

    await new Promise(resolve => {
      setTimeout(() => {
        writeScheduled = false;
        if (writePending) {
          writePending = false;
          fs.writeFileSync(
            path.join(TEST_DIR, 'new', 'forum.json'),
            JSON.stringify(state, null, 2)
          );
          writeCount++;
        }
        resolve();
      }, 100); // INBOUND_BATCH_TIMEOUT_MS
    });
  }

  (async () => {
    try {
      fs.mkdirSync(path.join(TEST_DIR, 'new'), { recursive: true });
      let forumState = {
        topics: [],
        posts: [],
        seenEnvelopeIds: []
      };

      for (const envelope of envelopes) {
        if (envelope.t === 'topic_create') {
          forumState.topics.push({
            topicId: envelope.topicId,
            title: envelope.title,
            bodyPreview: envelope.body.slice(0, 140),
            createdAt: new Date(envelope.ts).toISOString(),
            bumpedAt: new Date(envelope.ts).toISOString(),
          });
        } else {
          forumState.posts.push({
            postId: envelope.postId,
            topicId: envelope.topicId,
            body: envelope.body,
            createdAt: new Date(envelope.ts).toISOString(),
          });
        }
        forumState.seenEnvelopeIds.push(envelope.id);

        // NEW: Schedule batched async write
        await scheduleBatchedWrite(forumState);
      }

      // Wait for final batch
      await new Promise(resolve => setTimeout(resolve, 150));

      const durationAsync = Date.now() - startAsync;

      console.log(`⏱️  NEW (batched writes): ${formatDuration(durationAsync)}`);
      console.log(`📝 Disk writes: OLD=${messageCount}, NEW=${writeCount}`);
      console.log(`⚡ Speed improvement: ${(durationSync / durationAsync).toFixed(1)}x faster\n`);

      console.log('✅ Test complete!');
      console.log(`Results saved to: ${TEST_DIR}`);
    } catch (err) {
      console.error('Error in new approach:', err.message);
    }
  })();
}

// Run tests
console.log('='.repeat(50));
console.log('Forum Sync Performance Test');
console.log('='.repeat(50));

runTest(10);
runTest(100);
runTest(1000);
