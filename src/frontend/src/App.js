import React, { useState, useEffect, useRef } from 'react';
import './App.css';
import { API_BASE_URL } from './config';

const WORKFLOW_ID_PREFIX = 'strands-temporal-agent-';
const DEFAULT_HEADER_TEXT = 'Chat session started.';
// A typed reply to a pending confirmation counts as approval when it starts
// affirmatively; anything else is treated as a denial.
const AFFIRMATIVE = /^\s*(y|yes|yeah|yep|sure|ok|okay|approve|confirm|proceed|do it)\b/i;

function App() {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState('');
  const [isChatActive, setIsChatActive] = useState(false);
  const [statusContent, setStatusContent] = useState('');
  const [isPolling, setIsPolling] = useState(false);
  const [isWaitingForAIResponse, setIsWaitingForAIResponse] = useState(false);
  const [pendingApproval, setPendingApproval] = useState(null);

  const chatWindowRef = useRef(null);
  const eventCountRef = useRef(0);
  const workflowIdRef = useRef('');
  // User prompts echoed optimistically, awaiting reconciliation with server history.
  const pendingEchoesRef = useRef([]);
  // The approval question currently shown inline (so we don't re-add it each poll).
  const approvalShownRef = useRef(null);
  // An approval we've already answered, awaiting the workflow to clear it.
  const answeredApprovalRef = useRef(null);

  useEffect(() => {
    if (chatWindowRef.current) {
      chatWindowRef.current.scrollTop = chatWindowRef.current.scrollHeight;
    }
  }, [messages]);

  // Adaptive polling: faster (2s) while waiting for the assistant, slower (5s) otherwise.
  useEffect(() => {
    let intervalId;
    if (isPolling) {
      const poll = async () => {
        await fetchChatHistory();
        await fetchPendingApproval();
      };
      poll();
      const interval = isWaitingForAIResponse ? 2000 : 5000;
      intervalId = setInterval(poll, interval);
    }
    return () => clearInterval(intervalId);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isPolling, isWaitingForAIResponse]);

  const handleStartChat = async () => {
    try {
      const newSessionId = Math.random().toString(36).substring(2, 15);
      const newWorkflowId = WORKFLOW_ID_PREFIX + newSessionId;
      const response = await fetch(
        `${API_BASE_URL}/start-workflow?workflow_id=${newWorkflowId}`,
        { method: 'POST' }
      );
      const result = await response.json();
      if (response.ok && result.message === 'Workflow started.') {
        workflowIdRef.current = newWorkflowId;
        setMessages([{ text: DEFAULT_HEADER_TEXT, type: 'bot' }]);
        setStatusContent('');
        setPendingApproval(null);
        setIsChatActive(true);
        eventCountRef.current = 0;
        pendingEchoesRef.current = [];
        approvalShownRef.current = null;
        answeredApprovalRef.current = null;
        setIsPolling(true);
        setIsWaitingForAIResponse(false);
      } else {
        setMessages([{ text: `Workflow didn't start: ${result.message}`, type: 'bot' }]);
      }
    } catch (error) {
      console.error('Error starting chat:', error);
      setMessages([{ text: 'Failed to start chat session.', type: 'bot' }]);
    }
  };

  const handleSend = async () => {
    if (!input.trim() || !isChatActive) return;
    const text = input.trim();
    setInput('');
    // Echo the user's message inline immediately.
    setMessages((prev) => [...prev, { text, type: 'user' }]);
    setIsWaitingForAIResponse(true);

    // If the assistant is awaiting a delete/close confirmation, this message IS the
    // answer — route it to the approval signal so the confirmation stays inline in
    // the chat (no separate dialog box). The approval reply is not part of the
    // server chat history, so it is not tracked for reconciliation.
    if (pendingApproval) {
      const response = AFFIRMATIVE.test(text) ? 'approve' : 'deny';
      answeredApprovalRef.current = pendingApproval;
      setPendingApproval(null);
      try {
        await fetch(
          `${API_BASE_URL}/approve?workflow_id=${workflowIdRef.current}&response=${response}`,
          { method: 'POST' }
        );
      } catch (error) {
        console.error('Error sending approval:', error);
      }
      return;
    }

    // Normal prompt. Track the echo so the history merge doesn't duplicate it.
    pendingEchoesRef.current.push(text);
    try {
      await fetch(
        `${API_BASE_URL}/send-prompt?workflow_id=${workflowIdRef.current}&prompt=${encodeURIComponent(text)}`,
        { method: 'POST', headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      console.error('Error sending message:', error);
      setMessages((prev) => [...prev, { text: 'Failed to send message.', type: 'bot' }]);
    }
  };

  const handleEndChat = async () => {
    try {
      await fetch(`${API_BASE_URL}/end-chat?workflow_id=${workflowIdRef.current}`, {
        method: 'POST',
      });
    } catch (error) {
      console.error('Error ending chat:', error);
    }
    setIsPolling(false);
    setIsWaitingForAIResponse(false);
    setPendingApproval(null);
    setMessages((prev) => [...prev, { text: 'Chat session ended.', type: 'bot' }]);
    setIsChatActive(false);
  };

  const handleToggleChatState = () => {
    if (isChatActive) handleEndChat();
    else handleStartChat();
  };

  const fetchChatHistory = async () => {
    const from = eventCountRef.current;
    let data = null;
    try {
      const response = await fetch(
        `${API_BASE_URL}/get-chat-history?workflow_id=${workflowIdRef.current}&from_index=${from}`
      );
      data = await response.json();
    } catch (error) {
      console.error('Error fetching chat history:', error);
      return;
    }
    if (!data || data.length === 0) return;

    eventCountRef.current = from + data.length;
    const newMessages = [];
    let receivedBotResponse = false;
    data.forEach((item) => {
      switch (item.type) {
        case 'chat_interaction': {
          // If we already echoed this user prompt optimistically, add only the
          // assistant's response; otherwise add both. This tolerates messages
          // (e.g. an inline approval question/answer) interleaved between the
          // echo and the server's record of the turn.
          const userPrompt = item.content.user_prompt;
          const echoIdx = pendingEchoesRef.current.indexOf(userPrompt);
          if (echoIdx !== -1) {
            pendingEchoesRef.current.splice(echoIdx, 1);
            newMessages.push({ text: item.content.text_response, type: 'bot' });
          } else {
            newMessages.push(
              { text: userPrompt, type: 'user' },
              { text: item.content.text_response, type: 'bot' }
            );
          }
          receivedBotResponse = true;
          break;
        }
        case 'status_update':
          setStatusContent(item.content.status);
          break;
        default:
          break;
      }
    });

    if (receivedBotResponse) setIsWaitingForAIResponse(false);
    if (newMessages.length > 0) {
      setMessages((prev) => [...prev, ...newMessages]);
    }
  };

  const fetchPendingApproval = async () => {
    try {
      const response = await fetch(
        `${API_BASE_URL}/pending-approval?workflow_id=${workflowIdRef.current}`
      );
      const data = await response.json();
      const reason = data.pending || null;

      if (!reason) {
        answeredApprovalRef.current = null;
        approvalShownRef.current = null;
        setPendingApproval(null);
        return;
      }
      // Already answered this one; wait for the workflow to clear it.
      if (reason === answeredApprovalRef.current) return;

      setPendingApproval(reason);
      // Surface the confirmation question inline in the chat, exactly once. The
      // user answers by typing (handled in handleSend) — no separate dialog box.
      if (approvalShownRef.current !== reason) {
        approvalShownRef.current = reason;
        setMessages((prev) => [...prev, { text: reason, type: 'bot' }]);
      }
    } catch (error) {
      // ignore transient query errors
    }
  };

  return (
    <div className="App">
      <div className="header">Wealth Management Chatbot · Temporal + Strands</div>

      {statusContent && <div className="status-area">{statusContent}</div>}

      <div className="chat-window" ref={chatWindowRef}>
        {messages.map((msg, index) => (
          <div key={index} className={`message ${msg.type}`}>
            {(msg.text || '').split('\n').map((line, i) => (
              <span key={i}>
                {line}
                <br />
              </span>
            ))}
          </div>
        ))}
      </div>

      <div className="input-area">
        <input
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyPress={(e) => e.key === 'Enter' && handleSend()}
          placeholder="Type a message..."
          disabled={!isChatActive}
        />
        <button onClick={handleSend} disabled={!isChatActive}>Send</button>
      </div>

      <button
        onClick={handleToggleChatState}
        className={`end-chat-button ${!isChatActive ? 'start-chat-button' : ''}`}
      >
        {isChatActive ? 'End Chat' : 'Start Chat'}
      </button>
    </div>
  );
}

export default App;
