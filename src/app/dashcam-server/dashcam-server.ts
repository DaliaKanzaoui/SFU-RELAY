import {
  Component,
  ElementRef,
  OnInit,
  OnDestroy,
  ViewChild,
  AfterViewInit,
  signal,
  computed,
  ChangeDetectorRef
} from '@angular/core';
import { webSocket, WebSocketSubject } from 'rxjs/webSocket';
import { Subject, Subscription } from 'rxjs';
import { tap } from 'rxjs/operators';
import { v4 as uuidv4 } from 'uuid';
import { NgClass } from '@angular/common';
import { isPlatformBrowser } from '@angular/common';
import { Inject, PLATFORM_ID } from '@angular/core';
import { FormsModule } from '@angular/forms';

// Default Configuration
const DEFAULT_CONFIG = {
  peerConnectionConfig: {
    iceServers: [
      {
        urls: 'turn:coturn.teamusages.qa.protectline.fr:3478',
        username: 'coturnqa',
        credential: 'coturnqa'
      },
      {
        urls: 'stun:stun.l.google.com:19302'
      }
    ]
  },
  offerOptions: {
    offerToReceiveAudio: false,
    offerToReceiveVideo: false
  },
  pingIntervalMs: 20000
};

interface IceServerConfig {
  urls: string;
  username?: string;
  credential?: string;
}

interface Message {
  event: string;
  data?: any;
}

@Component({
  selector: 'app-dashcam-server',
  templateUrl: './dashcam-server.html',
  standalone: true,
  imports: [NgClass, FormsModule],
  styleUrls: ['./dashcam-server.scss']
})
export class DashcamServerComponent implements OnInit, OnDestroy, AfterViewInit {
  @ViewChild('local_video', { static: false }) videoElementRef!: ElementRef<HTMLVideoElement>;

  // User inputs - using signals
  websocketUrl = signal('wss://');
  streamId = signal('');
  
  // ICE Server Configuration - using signals
  showConfigPanel = signal(false);
  iceServers = signal<IceServerConfig[]>([]);
  editingIceServers = signal<IceServerConfig[]>([]);

  // Video source selection
  videoSource = signal<'webcam' | 'file'>('webcam');
  videoFile: File | null = null;

  // State variables - using signals
  videoElementReady = signal(false);
  connected = signal(false);
  streaming = signal(false);
  loading = signal(false);
  statusMessage = signal('');
  statusType = signal<'success' | 'info' | 'warning' | 'error'>('info');
  localStream: MediaStream | null = null;
  activeClients = signal<Set<string>>(new Set());

  // Computed signals
  canConnect = computed(() => {
    return this.websocketUrl().trim().length > 6 &&
      this.streamId().trim().length > 0 &&
      !this.connected() &&
      !this.loading();
  });

  canStartStreaming = computed(() => {
    return this.connected() && 
      (this.videoSource() === 'file' ? this.videoFile !== null : true) &&
      !this.streaming();
  });

  peerConnectionsCount = computed(() => this.peerConnections.size);

  // Internal variables
  private socket$: WebSocketSubject<Message> | null = null;
  private peerConnections: Map<string, RTCPeerConnection> = new Map();
  private messagesSubject$ = new Subject<any>();
  private messages$ = this.messagesSubject$.asObservable();
  private messageSubscription: Subscription | null = null;
  public myUsername: string = "";
  private pingInterval: any = null;
  private videoElement: HTMLVideoElement | null = null;
  private videoInitPromise: Promise<void> | null = null;
  private videoInitResolver: (() => void) | null = null;
  private initializationAttempts = 0;
  private readonly maxInitAttempts = 10;
  private videoObserver: MutationObserver | null = null;

  constructor(
    @Inject(PLATFORM_ID) private platformId: Object,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.myUsername = this.getBrowserId();
    console.log('Dashcam Server ID:', this.myUsername);

    this.createVideoInitPromise();
    this.loadConfiguration();

    if (isPlatformBrowser(this.platformId)) {
      const savedWsUrl = localStorage.getItem('dashcam_ws_url');
      const savedStreamId = localStorage.getItem('dashcam_stream_id');

      if (savedWsUrl) this.websocketUrl.set(savedWsUrl);
      if (savedStreamId) this.streamId.set(savedStreamId);
    }

    console.log('Dashcam Server Component initialized');
  }

  ngAfterViewInit(): void {
    console.log('ngAfterViewInit called');
    this.initializeVideoElementWithRetry();
    this.setupVideoObserver();
  }

  ngOnDestroy(): void {
    this.performEmergencyCleanup();
    if (this.videoObserver) {
      this.videoObserver.disconnect();
      this.videoObserver = null;
    }
  }

  // ============================================================================
  // ICE SERVER CONFIGURATION MANAGEMENT
  // ============================================================================

  private loadConfiguration(): void {
    if (!isPlatformBrowser(this.platformId)) {
      this.iceServers.set(DEFAULT_CONFIG.peerConnectionConfig.iceServers);
      return;
    }

    const savedConfig = localStorage.getItem('dashcam_ice_servers_config');
    if (savedConfig) {
      try {
        const parsed = JSON.parse(savedConfig);
        this.iceServers.set(parsed);
        console.log('Loaded ICE servers from localStorage:', parsed);
      } catch (error) {
        console.error('Error parsing saved ICE servers config:', error);
        this.iceServers.set(DEFAULT_CONFIG.peerConnectionConfig.iceServers);
        this.saveConfiguration();
      }
    } else {
      this.iceServers.set(DEFAULT_CONFIG.peerConnectionConfig.iceServers);
      this.saveConfiguration();
    }
  }

  private saveConfiguration(): void {
    if (!isPlatformBrowser(this.platformId)) return;

    try {
      const config = JSON.stringify(this.iceServers());
      localStorage.setItem('dashcam_ice_servers_config', config);
      console.log('Saved ICE servers to localStorage');
    } catch (error) {
      console.error('Error saving ICE servers config:', error);
    }
  }

  public toggleConfigPanel(): void {
    if (this.connected() || this.streaming()) {
      this.setStatus('Please disconnect before editing configuration', 'warning');
      return;
    }

    this.showConfigPanel.set(!this.showConfigPanel());

    if (this.showConfigPanel()) {
      this.editingIceServers.set(JSON.parse(JSON.stringify(this.iceServers())));
    }
  }

  public addIceServer(): void {
    const current = this.editingIceServers();
    this.editingIceServers.set([
      ...current,
      { urls: '', username: '', credential: '' }
    ]);
  }

  public removeIceServer(index: number): void {
    const current = this.editingIceServers();
    this.editingIceServers.set(current.filter((_, i) => i !== index));
  }

  public updateIceServerUrl(index: number, value: string): void {
    const current = [...this.editingIceServers()];
    current[index].urls = value;
    this.editingIceServers.set(current);
  }

  public updateIceServerUsername(index: number, value: string): void {
    const current = [...this.editingIceServers()];
    if (value.trim()) {
      current[index].username = value;
    } else {
      delete current[index].username;
    }
    this.editingIceServers.set(current);
  }

  public updateIceServerCredential(index: number, value: string): void {
    const current = [...this.editingIceServers()];
    if (value.trim()) {
      current[index].credential = value;
    } else {
      delete current[index].credential;
    }
    this.editingIceServers.set(current);
  }

  public saveIceServersConfig(): void {
    const edited = this.editingIceServers();

    const hasEmpty = edited.some(server => !server.urls.trim());
    if (hasEmpty) {
      this.setStatus('All ICE servers must have a URL', 'error');
      return;
    }

    const invalidUrls = edited.filter(server => {
      const url = server.urls.trim().toLowerCase();
      return !url.startsWith('stun:') && !url.startsWith('turn:') && !url.startsWith('turns:');
    });

    if (invalidUrls.length > 0) {
      this.setStatus('ICE server URLs must start with stun:, turn:, or turns:', 'error');
      return;
    }

    this.iceServers.set(edited);
    this.saveConfiguration();
    this.showConfigPanel.set(false);
    this.setStatus('ICE server configuration saved successfully', 'success');
  }

  public cancelIceServersEdit(): void {
    this.showConfigPanel.set(false);
    this.editingIceServers.set([]);
  }

  public resetToDefaults(): void {
    this.editingIceServers.set(JSON.parse(JSON.stringify(DEFAULT_CONFIG.peerConnectionConfig.iceServers)));
  }

  private getPeerConnectionConfig(): RTCConfiguration {
    return {
      iceServers: this.iceServers()
    };
  }

  // ============================================================================
  // VIDEO ELEMENT INITIALIZATION
  // ============================================================================

  private createVideoInitPromise(): void {
    this.videoInitPromise = new Promise<void>((resolve) => {
      this.videoInitResolver = resolve;
    });
  }

  private async waitForVideoElement(): Promise<void> {
    if (this.videoElementReady()) {
      return Promise.resolve();
    }
    return this.videoInitPromise || Promise.reject(new Error('Video initialization not started'));
  }

  private setupVideoObserver(): void {
    if (!isPlatformBrowser(this.platformId)) return;

    this.videoObserver = new MutationObserver(() => {
      if (!this.videoElementReady() && this.videoElementRef?.nativeElement) {
        console.log('MutationObserver detected video element');
        this.attemptVideoInitialization();
      }
    });

    const hostElement = document.querySelector('app-dashcam-server');
    if (hostElement) {
      this.videoObserver.observe(hostElement, {
        childList: true,
        subtree: true
      });
    }
  }

  private initializeVideoElementWithRetry(): void {
    this.initializationAttempts = 0;
    this.attemptVideoInitialization();
  }

  private attemptVideoInitialization(): void {
    this.initializationAttempts++;
    console.log(`Video initialization attempt ${this.initializationAttempts}/${this.maxInitAttempts}`);

    if (!this.videoElementRef) {
      console.warn('ViewChild not available yet');
      this.scheduleNextAttempt();
      return;
    }

    const nativeElement = this.videoElementRef.nativeElement;
    if (!nativeElement) {
      console.warn('Native element not available yet');
      this.scheduleNextAttempt();
      return;
    }

    if (nativeElement.tagName !== 'VIDEO') {
      console.error('Element is not a VIDEO tag:', nativeElement.tagName);
      this.scheduleNextAttempt();
      return;
    }

    if (!document.contains(nativeElement)) {
      console.warn('Video element not yet in DOM');
      this.scheduleNextAttempt();
      return;
    }

    console.log('✓ Video element validation successful');
    this.finalizeVideoInitialization(nativeElement);
  }

  private finalizeVideoInitialization(videoElement: HTMLVideoElement): void {
    try {
      this.videoElement = videoElement;

      videoElement.autoplay = true;
      videoElement.playsInline = true;
      videoElement.muted = true;
      videoElement.controls = false;

      this.videoElementReady.set(true);

      if (this.videoInitResolver) {
        this.videoInitResolver();
        this.videoInitResolver = null;
      }

      this.cdr.detectChanges();
      console.log('✓✓✓ Video element fully initialized and ready ✓✓✓');
      this.setStatus('Video element ready', 'success');

    } catch (error) {
      console.error('Error during video finalization:', error);
      this.scheduleNextAttempt();
    }
  }

  private scheduleNextAttempt(): void {
    if (this.initializationAttempts >= this.maxInitAttempts) {
      console.error('❌ Failed to initialize video element after maximum attempts');
      this.setStatus('Video element initialization failed', 'error');
      return;
    }

    const delay = Math.min(50 * Math.pow(2, this.initializationAttempts - 1), 2000);
    setTimeout(() => {
      this.attemptVideoInitialization();
    }, delay);
  }

  private getVideoElement(): HTMLVideoElement | null {
    if (!this.videoElementReady()) {
      console.error('Attempted to get video element before it was ready');
      return null;
    }
    return this.videoElement || this.videoElementRef?.nativeElement || null;
  }

  // ============================================================================
  // VIDEO SOURCE MANAGEMENT
  // ============================================================================

  async startVideoSource(): Promise<void> {
    if (!this.videoElementReady()) {
      this.setStatus('Initializing video element...', 'info');
      try {
        await this.waitForVideoElement();
      } catch (error) {
        this.setStatus('Video element not ready. Please refresh the page.', 'error');
        return;
      }
    }

    this.loading.set(true);

    try {
      if (this.videoSource() === 'webcam') {
        await this.startWebcam();
      } else {
        await this.startVideoFile();
      }
    } catch (error: any) {
      console.error('Error starting video source:', error);
      this.setStatus('Failed to start video source: ' + (error.message || 'Unknown error'), 'error');
      this.loading.set(false);
    }
  }

  private async startWebcam(): Promise<void> {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({
        video: { width: 1280, height: 720 },
        audio: false
      });

      this.localStream = stream;
      const videoElement = this.getVideoElement();
      if (videoElement) {
        videoElement.srcObject = stream;
        await videoElement.play();
      }

      this.streaming.set(true);
      this.loading.set(false);
      this.setStatus('Webcam stream started', 'success');
    } catch (error: any) {
      console.error('Error accessing webcam:', error);
      this.setStatus('Failed to access webcam: ' + (error.message || 'Permission denied'), 'error');
      throw error;
    }
  }

  private async startVideoFile(): Promise<void> {
    if (!this.videoFile) {
      this.setStatus('Please select a video file first', 'warning');
      this.loading.set(false);
      return;
    }

    try {
      const videoElement = this.getVideoElement();
      if (!videoElement) {
        throw new Error('Video element not available');
      }

      const url = URL.createObjectURL(this.videoFile);
      videoElement.src = url;
      videoElement.loop = true;
      await videoElement.play();

      // Capture stream from video element
      // TypeScript doesn't have captureStream in HTMLVideoElement types, so we use type assertion
      const videoElementWithCapture = videoElement as HTMLVideoElement & {
        captureStream?: () => MediaStream;
      };
      const stream = videoElementWithCapture.captureStream ? videoElementWithCapture.captureStream() : null;
      if (!stream) {
        throw new Error('Video element does not support captureStream');
      }

      this.localStream = stream;
      this.streaming.set(true);
      this.loading.set(false);
      this.setStatus('Video file stream started', 'success');
    } catch (error: any) {
      console.error('Error starting video file:', error);
      this.setStatus('Failed to start video file: ' + (error.message || 'Unknown error'), 'error');
      throw error;
    }
  }

  stopVideoSource(): void {
    if (this.localStream) {
      this.localStream.getTracks().forEach(track => track.stop());
      this.localStream = null;
    }

    const videoElement = this.getVideoElement();
    if (videoElement) {
      videoElement.srcObject = null;
      videoElement.src = '';
      videoElement.load();
    }

    this.streaming.set(false);
    this.setStatus('Video source stopped', 'info');
  }

  onVideoFileSelected(event: Event): void {
    const input = event.target as HTMLInputElement;
    if (input.files && input.files.length > 0) {
      this.videoFile = input.files[0];
      this.setStatus(`Video file selected: ${this.videoFile.name}`, 'success');
    }
  }

  setVideoSource(source: 'webcam' | 'file'): void {
    if (this.streaming()) {
      this.setStatus('Please stop streaming before changing video source', 'warning');
      return;
    }
    this.videoSource.set(source);
  }

  // ============================================================================
  // WEBSOCKET CONNECTION
  // ============================================================================

  public connect(): void {
    if (!this.canConnect()) {
      if (!this.websocketUrl().trim() || this.websocketUrl() === 'wss://') {
        this.setStatus('Please enter WebSocket URL', 'warning');
      } else if (!this.streamId().trim()) {
        this.setStatus('Please enter Stream ID', 'warning');
      }
      return;
    }

    if (isPlatformBrowser(this.platformId)) {
      localStorage.setItem('dashcam_ws_url', this.websocketUrl());
      localStorage.setItem('dashcam_stream_id', this.streamId());
    }

    this.loading.set(true);
    this.setStatus('Connecting to server...', 'info');

    try {
      this.socket$ = this.getNewWebSocket();

      this.socket$.subscribe(
        (msg: any) => {
          this.messagesSubject$.next(msg);
          this.loading.set(false);
        },
        (error: any) => {
          console.error('WebSocket error:', error);
          this.connected.set(false);
          this.loading.set(false);
          this.stopPing();
          this.setStatus('Connection error: ' + (error.message || 'Unknown'), 'error');
        },
        () => {
          console.log('WebSocket connection closed');
          this.connected.set(false);
          this.loading.set(false);
          this.stopPing();
          this.setStatus('Connection closed', 'warning');
        }
      );
    } catch (error: any) {
      console.error('Error creating WebSocket:', error);
      this.loading.set(false);
      this.setStatus('Failed to connect: ' + error.message, 'error');
    }
  }

  private getNewWebSocket(): WebSocketSubject<any> {
    return webSocket({
      url: this.websocketUrl(),
      openObserver: {
        next: () => {
          console.log('WebSocket connection established successfully');
          this.connected.set(true);
          this.loading.set(false);
          this.setStatus('Connection established', 'success');
          this.registerWithServer();
          this.setupMessageHandlers();
        }
      },
      closeObserver: {
        next: (event) => {
          console.log('WebSocket connection closed', event.reason);
          this.connected.set(false);
          this.stopPing();
          this.setStatus('Connection closed: ' + (event.reason || 'Unknown reason'), 'warning');
        }
      }
    });
  }

  private registerWithServer(): void {
    this.sendMessage({
      event: 'register-client',
      data: {
        id: this.streamId(),
        name: this.streamId(),
        date: Date.now(),
        type: 'pingv2',
        peerType: 'dashcam' // Register as dashcam server
      }
    });
    this.startPing();
  }

  private sendMessage(msg: any): void {
    if (!this.socket$) {
      console.error('Cannot send message: not connected');
      return;
    }
    console.log('Sending message:', msg);
    this.socket$.next(msg);
  }

  // ============================================================================
  // PING/PONG
  // ============================================================================

  private startPing(): void {
    this.stopPing();
    console.log(`Starting pingv2 mode: sending PING every ${DEFAULT_CONFIG.pingIntervalMs}ms`);

    this.sendPing();

    this.pingInterval = setInterval(() => {
      if (this.connected() && this.socket$) {
        console.log('Sending periodic PING (pingv2)');
        this.sendPing();
      } else {
        console.warn('Connection lost, stopping ping interval');
        this.stopPing();
      }
    }, DEFAULT_CONFIG.pingIntervalMs);
  }

  private stopPing(): void {
    if (this.pingInterval) {
      clearInterval(this.pingInterval);
      this.pingInterval = null;
      console.log('Stopped pingv2 interval');
    }
  }

  private sendPing(): void {
    this.sendMessage({
      event: 'ping',
      data: { name: this.streamId() }
    });
  }

  // ============================================================================
  // MESSAGE HANDLING
  // ============================================================================

  private setupMessageHandlers(): void {
    if (this.messageSubscription) {
      this.messageSubscription.unsubscribe();
    }

    console.log("Subscribing to incoming messages");
    this.messageSubscription = this.messages$.pipe(
      tap(msg => {
        if (msg.data != null) {
          const msgType = msg.data.type;
          console.log(`Received message of type: ${msgType}`);

          switch (msgType) {
            case 'video-request':
              this.handleVideoRequest(msg.data);
              break;
            case 'hangup':
            case 'hang-up':
              this.handleHangupMessage(msg.data);
              break;
            case 'ice-candidate':
            case 'new-ice-candidate':
              this.handleIceCandidate(msg.data);
              break;
            case 'video-offer':
              this.handleVideoOffer(msg.data);
              break;
            case 'video-answer':
              this.handleVideoAnswer(msg.data);
              break;
            case 'error':
              console.log("Error message:", JSON.stringify(msg.data));
              this.setStatus('Server error: ' + (msg.data.message || 'Unknown error'), 'error');
              break;
            default:
              console.log('Unhandled message type', msgType);
              break;
          }
        } else if (msg.event == "pong") {
          console.log('Received PONG from server (pingv2)');
        } else if (msg.event == "ping") {
          console.log('Received PING from server (legacy mode)');
          this.sendMessage({
            event: "pong",
            data: {name: this.streamId()}
          });
        }
      })
    ).subscribe(
      () => {},
      (error) => {
        console.error('Error in message subscription:', error);
        this.setStatus('Connection error: Failed to process messages', 'error');
        this.stopPing();
      }
    );
  }

  private async handleVideoRequest(data: any): Promise<void> {
    const clientId = data.source;
    console.log(`Received video request from client: ${clientId}`);

    if (!this.localStream) {
      this.setStatus('No video source available. Please start streaming first.', 'error');
      return;
    }

    if (this.peerConnections.has(clientId)) {
      console.log(`Peer connection already exists for client: ${clientId}`);
      return;
    }

    this.setStatus(`Client ${clientId} requested video stream`, 'info');
    await this.createPeerConnectionForClient(clientId);
  }

  private async createPeerConnectionForClient(clientId: string): Promise<void> {
    try {
      const peerConnection = new RTCPeerConnection(this.getPeerConnectionConfig());

      // Add local stream tracks
      if (this.localStream) {
        this.localStream.getTracks().forEach(track => {
          peerConnection.addTrack(track, this.localStream!);
          console.log(`Added ${track.kind} track to peer connection`);
        });
      }

      // Set up event handlers
      peerConnection.onicecandidate = (event) => {
        if (event.candidate) {
          console.log('Outgoing ICE candidate:', JSON.stringify(event.candidate, null, 2));
          this.sendMessage({
            event: 'message',
            data: {
              candidate: event.candidate,
              source: this.streamId(),
              target: clientId,
              type: 'new-ice-candidate'
            }
          });
        }
      };

      peerConnection.oniceconnectionstatechange = () => {
        if (!peerConnection) return;
        console.log('ICE connection state changed to:', peerConnection.iceConnectionState);

        switch (peerConnection.iceConnectionState) {
          case 'connected':
            this.setStatus(`Client ${clientId} connected`, 'success');
            const clients = new Set(this.activeClients());
            clients.add(clientId);
            this.activeClients.set(clients);
            break;
          case 'disconnected':
          case 'failed':
          case 'closed':
            this.setStatus(`Client ${clientId} disconnected`, 'info');
            const updatedClients = new Set(this.activeClients());
            updatedClients.delete(clientId);
            this.activeClients.set(updatedClients);
            this.peerConnections.delete(clientId);
            peerConnection.close();
            break;
        }
      };

      peerConnection.onsignalingstatechange = () => {
        if (!peerConnection) return;
        console.log('WebRTC signaling state changed to:', peerConnection.signalingState);
      };

      // Create and send offer
      const offer = await peerConnection.createOffer(DEFAULT_CONFIG.offerOptions);
      await peerConnection.setLocalDescription(offer);

      console.log('Sending video offer to client:', clientId);
      this.sendMessage({
        event: 'message',
        data: {
          source: this.streamId(),
          target: clientId,
          type: 'video-offer',
          sdp: peerConnection.localDescription
        }
      });

      this.peerConnections.set(clientId, peerConnection);
    } catch (error) {
      console.error('Error creating peer connection for client:', error);
      this.setStatus(`Failed to create connection for client ${clientId}`, 'error');
    }
  }

  private async handleVideoOffer(data: any): Promise<void> {
    // As a server, we typically don't receive offers, but handle it just in case
    console.log('Received video offer (unexpected for server)');
  }

  private async handleVideoAnswer(data: any): Promise<void> {
    const clientId = data.source;
    console.log(`Received video answer from client: ${clientId}`);

    const peerConnection = this.peerConnections.get(clientId);
    if (!peerConnection) {
      console.error(`No peer connection found for client: ${clientId}`);
      return;
    }

    try {
      const desc = new RTCSessionDescription(data.sdp);
      await peerConnection.setRemoteDescription(desc);
      console.log('Set remote description from client answer');
      this.setStatus(`Client ${clientId} accepted video stream`, 'success');
    } catch (error) {
      console.error('Error handling video answer:', error);
      this.setStatus(`Failed to process answer from client ${clientId}`, 'error');
    }
  }

  private async handleIceCandidate(data: any): Promise<void> {
    const clientId = data.source;
    const peerConnection = this.peerConnections.get(clientId);

    if (!peerConnection) {
      console.warn(`No peer connection found for client: ${clientId}`);
      return;
    }

    try {
      console.log('Incoming ICE candidate from client:', clientId);
      await peerConnection.addIceCandidate(new RTCIceCandidate(data.candidate));
      console.log('Successfully added incoming ICE candidate');
    } catch (error) {
      console.error('Error adding ICE candidate:', error);
    }
  }

  private handleHangupMessage(data: any): void {
    const clientId = data.source;
    console.log(`Received hangup from client: ${clientId}`);

    const peerConnection = this.peerConnections.get(clientId);
    if (peerConnection) {
      peerConnection.close();
      this.peerConnections.delete(clientId);
    }

    const clients = new Set(this.activeClients());
    clients.delete(clientId);
    this.activeClients.set(clients);
    this.setStatus(`Client ${clientId} disconnected`, 'info');
  }

  // ============================================================================
  // DISCONNECT
  // ============================================================================

  public disconnect(): void {
    console.log('Disconnecting from server - full cleanup');

    try {
      this.stopPing();
      this.stopVideoSource();

      // Close all peer connections
      this.peerConnections.forEach((pc, clientId) => {
        try {
          pc.close();
        } catch (error) {
          console.warn(`Error closing peer connection for ${clientId}:`, error);
        }
      });
      this.peerConnections.clear();
      this.activeClients.set(new Set());

      if (this.socket$ && this.connected()) {
        try {
          this.sendMessage({
            event: 'message',
            data: {
              source: this.streamId(),
              target: 'server',
              type: 'disconnect'
            }
          });
          console.log('Sent disconnect message to server');
        } catch (error) {
          console.warn('Error sending disconnect message:', error);
        }
      }

      if (this.messageSubscription) {
        try {
          this.messageSubscription.unsubscribe();
          this.messageSubscription = null;
        } catch (error) {
          console.warn('Error unsubscribing from messages:', error);
        }
      }

      if (this.socket$) {
        try {
          this.socket$.complete();
        } catch (error) {
          console.warn('Error closing WebSocket:', error);
        } finally {
          this.socket$ = null;
        }
      }

      this.connected.set(false);
      this.streaming.set(false);
      this.loading.set(false);

      this.messagesSubject$ = new Subject<any>();
      this.messages$ = this.messagesSubject$.asObservable();

      this.setStatus('Disconnected - Ready for new connection', 'info');

    } catch (error) {
      console.error('Error during disconnect:', error);
      this.socket$ = null;
      this.messageSubscription = null;
      this.connected.set(false);
      this.streaming.set(false);
      this.stopPing();
      this.setStatus('Disconnected (with errors)', 'warning');
    }
  }

  // ============================================================================
  // CLEANUP METHODS
  // ============================================================================

  private performEmergencyCleanup(): void {
    console.log('Emergency cleanup triggered');
    try {
      this.stopPing();
      this.stopVideoSource();

      this.peerConnections.forEach((pc) => {
        try {
          pc.close();
        } catch (e) {
          console.warn('Error closing peer connection:', e);
        }
      });
      this.peerConnections.clear();
      this.activeClients.set(new Set());

      if (this.socket$) {
        try {
          this.socket$.complete();
        } catch (e) {
          console.warn('Error completing socket:', e);
        }
      }

      if (this.messageSubscription) {
        try {
          this.messageSubscription.unsubscribe();
        } catch (e) {
          console.warn('Error unsubscribing:', e);
        }
      }
    } catch (error) {
      console.error('Error during emergency cleanup:', error);
    }
  }

  // ============================================================================
  // UTILITY METHODS
  // ============================================================================

  private getBrowserId(): string {
    if (!isPlatformBrowser(this.platformId)) {
      return uuidv4();
    }

    let browserId = localStorage.getItem('dashcam_browser_id');
    if (!browserId) {
      browserId = uuidv4();
      localStorage.setItem('dashcam_browser_id', browserId);
    }
    return browserId;
  }

  private setStatus(message: string, type: 'success' | 'info' | 'warning' | 'error'): void {
    this.statusMessage.set(message);
    this.statusType.set(type);
    console.log(`[${type.toUpperCase()}] ${message}`);
  }

  onWebsocketUrlChange(value: string): void {
    this.websocketUrl.set(value);
    this.statusMessage.set('');
  }

  onStreamIdChange(value: string): void {
    this.streamId.set(value);
    this.statusMessage.set('');
  }
}
