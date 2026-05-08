// video.component.ts - WITH ULTRA-FAST SEAMLESS SFU FAILOVER (double-buffering)
// FIX: handleBecomeBackupSfu now connects to camera FIRST before sending backup-sfu-accept
// FIX ÉCRAN NOIR: cameraPc.ontrack now sets videoElement.srcObject on the backup SFU itself
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
    offerToReceiveAudio: true,
    offerToReceiveVideo: true
  },
  pingIntervalMs: 20000,
  streamCameras: {
    driver: 'S5',
    front: 'S4',
    rear: 'S6'
  },
  playbackCameras: {
    driver: 'CH2',
    front: 'CH1',
    rear: 'CH3'
  },
  wakeupRetryIntervalMs: 10000,
  wakeupTimeoutMs: 300000,
  // Configuration pour switch SFU ultra-rapide - TIMEOUT REDUIT A 500ms
  sfuReconnectTimeoutMs: 500,
  sfuReconnectMaxAttempts: 2,
  preloadBackupSfu: true,
  preloadStartDelayMs: 100,
  // Timeout pour la connexion backup SFU → caméra avant d'envoyer backup-sfu-accept
  backupSfuCameraConnectTimeoutMs: 10000
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

interface PendingVideoRequest {
  camera: 'driver' | 'front' | 'rear';
  infos: string;
  isPlayback: boolean;
}

// ============================================================================
// État de la connexion backup SFU → caméra
//
// Le backup SFU doit d'abord établir une session WebRTC avec la caméra,
// puis envoyer backup-sfu-accept. On suit l'état de ce processus ici.
// ============================================================================
interface BackupSfuState {
  cameraTarget: string;
  failedSfuId: string;
  infos: string;
  // true = on attend le video-offer de la caméra (après avoir envoyé video-request)
  waitingForCameraOffer: boolean;
  // true = session WebRTC avec la caméra établie, prêt à notifier le serveur
  cameraSessionReady: boolean;
  // Peer connection dédiée à la connexion avec la caméra (en tant que backup SFU)
  cameraPeerConnection: RTCPeerConnection | null;
  // Timer de timeout pour la connexion caméra
  connectTimeoutTimer: any;
}

@Component({
  selector: 'app-video',
  templateUrl: './video.html',
  standalone: true,
  imports: [NgClass, FormsModule],
  styleUrls: ['./video.scss']
})
export class VideoComponent implements OnInit, OnDestroy, AfterViewInit {
  @ViewChild('remote_video', { static: false }) videoElementRef!: ElementRef<HTMLVideoElement>;

  // User inputs
  websocketUrl = signal('wss://');
  targetStreamId = signal('');
  mode = signal<'stream' | 'playback'>('stream');
  playbackDate = signal('');
  playbackTime = signal('');

  // ICE Server Configuration
  showConfigPanel = signal(false);
  iceServers = signal<IceServerConfig[]>([]);
  editingIceServers = signal<IceServerConfig[]>([]);

  // State variables
  videoElementReady = signal(false);
  connected = signal(false);
  streaming = signal(false);
  loading = signal(false);
  currentCamera = signal<'driver' | 'front' | 'rear' | null>(null);
  statusMessage = signal('');
  statusType = signal<'success' | 'info' | 'warning' | 'error'>('info');
  isWakingUp = signal(false);
  wakeupTimeRemaining = signal(0);

  // SFU Relay signals
  isSfuRelay = signal(false);
  activeViewers = signal<number>(0);

  // Signaux pour la reconnexion SFU transparente
  isReconnectingSfu = signal(false);
  reconnectAttempts = signal(0);

  // Signaux pour préchargement SFU
  isPreloadingBackupSfu = signal(false);
  backupSfuReady = signal(false);

  // Computed signals
  canConnect = computed(() => {
    return this.websocketUrl().trim().length > 6 &&
      this.targetStreamId().trim().length > 0 &&
      !this.connected() &&
      !this.loading();
  });

  canStartPlayback = computed(() => {
    if (this.mode() !== 'playback') return true;
    return this.playbackDate().trim().length > 0 &&
      this.playbackTime().trim().length > 0;
  });

  isDriverActive = computed(() => this.currentCamera() === 'driver');
  isFrontActive = computed(() => this.currentCamera() === 'front');
  isRearActive = computed(() => this.currentCamera() === 'rear');
  isStreamMode = computed(() => this.mode() === 'stream');
  isPlaybackMode = computed(() => this.mode() === 'playback');
  relayStatus = computed(() =>
    this.isSfuRelay()
      ? `🔄 Relaying to ${this.activeViewers()} viewer(s)`
      : '📡 Regular viewer'
  );

  // Internal variables
  private socket$: WebSocketSubject<Message> | null = null;
  private peerConnection: RTCPeerConnection | null = null;
  private messagesSubject$ = new Subject<any>();
  private messages$ = this.messagesSubject$.asObservable();
  private messageSubscription: Subscription | null = null;
  public myUsername: string = '';
  private pingInterval: any = null;
  public wakeupRetryInterval: any = null;
  private wakeupTimeoutTimer: any = null;
  private pendingVideoRequest: PendingVideoRequest | null = null;
  private wakeupStartTime: number = 0;
  private videoElement: HTMLVideoElement | null = null;
  private videoInitPromise: Promise<void> | null = null;
  private videoInitResolver: (() => void) | null = null;
  private initializationAttempts = 0;
  private readonly maxInitAttempts = 10;
  private videoObserver: MutationObserver | null = null;

  // SFU Relay specific
  private sfuPeerConnections: Map<string, RTCPeerConnection> = new Map();
  private remoteStream: MediaStream | null = null;
  private isRelayMode = false;
  private currentSfuId: string | null = null;
  private nextSfuId: string | null = null;

  // Timer de reconnexion SFU
  private sfuReconnectTimer: any = null;

  // ============================================================================
  // État du processus "become-backup-sfu"
  //
  // On garde la trace de l'état de la connexion backup SFU → caméra.
  // backup-sfu-accept n'est envoyé qu'APRÈS que ontrack soit déclenché
  // sur la cameraPeerConnection (stream caméra reçu).
  // ============================================================================
  private backupSfuState: BackupSfuState | null = null;

  // ============================================================================
  // DOUBLE-BUFFERING SFU - Pour switch ultra-rapide
  // ============================================================================
  private backupPeerConnection: RTCPeerConnection | null = null;
  private backupStream: MediaStream | null = null;
  private backupSfuId: string | null = null;
  private isBackupPreloading = false;
  private pendingBackupIceCandidates: RTCIceCandidateInit[] = [];

  constructor(
    @Inject(PLATFORM_ID) private platformId: Object,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.myUsername = this.getBrowserId();
    console.log('Browser ID:', this.myUsername);

    this.createVideoInitPromise();
    this.loadConfiguration();

    if (isPlatformBrowser(this.platformId)) {
      const savedWsUrl = localStorage.getItem('test_ws_url');
      const savedTargetId = localStorage.getItem('test_target_id');
      const savedMode = localStorage.getItem('test_mode');

      if (savedWsUrl) this.websocketUrl.set(savedWsUrl);
      if (savedTargetId) this.targetStreamId.set(savedTargetId);
      if (savedMode) this.mode.set(savedMode as 'stream' | 'playback');
    }

    console.log('Component initialized');
  }

  ngAfterViewInit(): void {
    console.log('ngAfterViewInit called');
    this.initializeVideoElementWithRetry();
    this.setupVideoObserver();
  }

  ngOnDestroy(): void {
    this.performEmergencyCleanup();
    this.clearWakeupRetry();
    this.cleanupSfuConnections();
    this.clearSfuReconnect();
    this.cleanupBackupConnection();
    this.cleanupBackupSfuState();

    if (this.videoObserver) {
      this.videoObserver.disconnect();
      this.videoObserver = null;
    }
  }

  // ============================================================================
  // CLEANUP BACKUP SFU STATE
  // ============================================================================

  private cleanupBackupSfuState(): void {
    if (!this.backupSfuState) return;

    if (this.backupSfuState.connectTimeoutTimer) {
      clearTimeout(this.backupSfuState.connectTimeoutTimer);
    }

    if (this.backupSfuState.cameraPeerConnection) {
      try {
        this.backupSfuState.cameraPeerConnection.ontrack = null;
        this.backupSfuState.cameraPeerConnection.onicecandidate = null;
        this.backupSfuState.cameraPeerConnection.oniceconnectionstatechange = null;
        this.backupSfuState.cameraPeerConnection.close();
      } catch (e) {
        console.warn('Error closing backup SFU camera peer connection:', e);
      }
    }

    this.backupSfuState = null;
    console.log('🧹 Backup SFU state cleaned up');
  }

  // ============================================================================
  // CLEANUP BACKUP (DOUBLE-BUFFERING)
  // ============================================================================

  private cleanupBackupConnection(): void {
    if (this.backupPeerConnection) {
      try {
        this.backupPeerConnection.ontrack = null;
        this.backupPeerConnection.onicecandidate = null;
        this.backupPeerConnection.oniceconnectionstatechange = null;
        this.backupPeerConnection.close();
      } catch (e) {
        console.warn('Error closing backup connection:', e);
      }
      this.backupPeerConnection = null;
    }

    if (this.backupStream) {
      this.backupStream.getTracks().forEach(track => track.stop());
      this.backupStream = null;
    }

    this.backupSfuId = null;
    this.isBackupPreloading = false;
    this.pendingBackupIceCandidates = [];
    this.isPreloadingBackupSfu.set(false);
    this.backupSfuReady.set(false);
  }

  // ============================================================================
  // DOUBLE-BUFFERING - Préchargement du backup SFU
  // ============================================================================

  private async preloadBackupSfu(sfuId: string, infos?: string): Promise<void> {
    if (this.isBackupPreloading || this.backupSfuId === sfuId) {
      console.log(`📦 Backup preload already in progress for SFU ${sfuId}`);
      return;
    }

    if (this.currentSfuId === sfuId) {
      console.log(`📦 SFU ${sfuId} is current, skipping backup preload`);
      return;
    }

    console.log(`🔮 DOUBLE-BUFFERING: Preloading backup SFU ${sfuId} for instant failover`);
    this.isBackupPreloading = true;
    this.isPreloadingBackupSfu.set(true);
    this.backupSfuId = sfuId;

    try {
      this.cleanupBackupConnection();

      this.backupPeerConnection = new RTCPeerConnection(this.getPeerConnectionConfig());

      this.backupPeerConnection.ontrack = (event) => {
        if (event.streams && event.streams[0]) {
          console.log(`✅ Backup stream received from SFU ${sfuId}`);
          this.backupStream = event.streams[0];
          this.backupSfuReady.set(true);
          this.isPreloadingBackupSfu.set(false);

          if (this.pendingBackupIceCandidates.length > 0) {
            console.log(`Processing ${this.pendingBackupIceCandidates.length} pending ICE candidates`);
            for (const candidate of this.pendingBackupIceCandidates) {
              this.backupPeerConnection?.addIceCandidate(new RTCIceCandidate(candidate))
                .catch(e => console.warn('Error adding pending ICE candidate:', e));
            }
            this.pendingBackupIceCandidates = [];
          }
        }
      };

      this.backupPeerConnection.onicecandidate = (event) => {
        if (event.candidate) {
          this.sendMessage({
            event: 'message',
            data: {
              source: this.myUsername,
              target: sfuId,
              type: 'new-ice-candidate',
              candidate: event.candidate,
              isBackup: true
            }
          });
        }
      };

      this.backupPeerConnection.oniceconnectionstatechange = () => {
        const state = this.backupPeerConnection?.iceConnectionState;
        console.log(`📦 Backup ICE state for ${sfuId}: ${state}`);

        if (state === 'connected') {
          console.log(`✅ Backup connection to ${sfuId} established - ready for instant switch`);
        } else if (state === 'failed') {
          console.warn(`⚠️ Backup connection to ${sfuId} failed`);
          this.cleanupBackupConnection();
        }
      };

      await new Promise(resolve => setTimeout(resolve, DEFAULT_CONFIG.preloadStartDelayMs));

      const offer = await this.backupPeerConnection.createOffer({
        offerToReceiveAudio: true,
        offerToReceiveVideo: true
      });
      await this.backupPeerConnection.setLocalDescription(offer);

      this.sendMessage({
        event: 'message',
        data: {
          source: this.myUsername,
          target: sfuId,
          type: 'backup-preload-request',
          sdp: this.backupPeerConnection.localDescription,
          infos: infos
        }
      });

      console.log(`📤 Backup preload request sent to SFU ${sfuId}`);

    } catch (error) {
      console.error(`Failed to preload backup SFU ${sfuId}:`, error);
      this.cleanupBackupConnection();
    }
  }

  private atomicSwitchToBackup(): boolean {
    if (!this.backupStream || !this.backupSfuReady()) {
      console.warn('No backup stream available for atomic switch');
      return false;
    }

    const videoElement = this.getVideoElement();
    if (!videoElement) {
      console.error('Video element not available');
      return false;
    }

    console.log(`⚡ ATOMIC SFU SWITCH! Switching to backup SFU ${this.backupSfuId}`);

    try {
      const oldStream = videoElement.srcObject as MediaStream;

      videoElement.srcObject = this.backupStream;

      const playPromise = videoElement.play();
      if (playPromise) {
        playPromise.catch(e => console.warn('Play after atomic switch:', e));
      }

      if (oldStream && oldStream !== this.backupStream) {
        setTimeout(() => {
          oldStream.getTracks().forEach(track => {
            if (track.readyState === 'live') {
              track.stop();
            }
          });
        }, 1000);
      }

      this.remoteStream = this.backupStream;

      const oldSfuId = this.currentSfuId;
      this.currentSfuId = this.backupSfuId;
      this.nextSfuId = null;

      this.cleanupBackupConnection();

      console.log(`✅ Atomic SFU switch completed: ${oldSfuId} → ${this.currentSfuId}`);
      return true;

    } catch (error) {
      console.error('Error during atomic switch:', error);
      return false;
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

    const savedConfig = localStorage.getItem('ice_servers_config');
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
      localStorage.setItem('ice_servers_config', config);
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
    this.editingIceServers.set(
      JSON.parse(JSON.stringify(DEFAULT_CONFIG.peerConnectionConfig.iceServers))
    );
  }

  private getPeerConnectionConfig(): RTCConfiguration {
    return {
      iceServers: this.iceServers()
    };
  }

  // ============================================================================
  // WAKE-UP RETRY MECHANISM
  // ============================================================================

  private startWakeupRetry(
    camera: 'driver' | 'front' | 'rear',
    infos: string,
    isPlayback: boolean
  ): void {
    console.log('Starting wake-up retry mechanism');

    this.pendingVideoRequest = { camera, infos, isPlayback };
    this.isWakingUp.set(true);
    this.wakeupStartTime = Date.now();
    this.wakeupTimeRemaining.set(Math.ceil(DEFAULT_CONFIG.wakeupTimeoutMs / 1000));

    const countdownInterval = setInterval(() => {
      const elapsed = Date.now() - this.wakeupStartTime;
      const remaining = Math.ceil((DEFAULT_CONFIG.wakeupTimeoutMs - elapsed) / 1000);
      this.wakeupTimeRemaining.set(Math.max(0, remaining));
      this.cdr.detectChanges();
    }, 1000);

    this.wakeupRetryInterval = setInterval(() => {
      console.log('Retrying video request during wake-up...');
      this.retryVideoRequest();
    }, DEFAULT_CONFIG.wakeupRetryIntervalMs);

    this.wakeupTimeoutTimer = setTimeout(() => {
      console.error('Wake-up timeout reached');
      clearInterval(countdownInterval);
      this.handleWakeupTimeout();
    }, DEFAULT_CONFIG.wakeupTimeoutMs);

    this.retryVideoRequest();
  }

  private retryVideoRequest(): void {
    if (!this.pendingVideoRequest) {
      console.warn('No pending video request to retry');
      return;
    }

    const { infos } = this.pendingVideoRequest;

    console.log('Sending video request retry with infos:', infos);

    this.sendMessage({
      event: 'message',
      data: {
        source: this.myUsername,
        type: 'video-request',
        infos: infos,
        target: this.targetStreamId()
      }
    });
  }

  private clearWakeupRetry(): void {
    console.log('Clearing wake-up retry mechanism');

    if (this.wakeupRetryInterval) {
      clearInterval(this.wakeupRetryInterval);
      this.wakeupRetryInterval = null;
    }

    if (this.wakeupTimeoutTimer) {
      clearTimeout(this.wakeupTimeoutTimer);
      this.wakeupTimeoutTimer = null;
    }

    this.isWakingUp.set(false);
    this.wakeupTimeRemaining.set(0);
    this.pendingVideoRequest = null;
    this.wakeupStartTime = 0;
  }

  private handleWakeupSuccess(): void {
    console.log('Wake-up successful, stream starting');
    this.clearWakeupRetry();
    this.setStatus('Camera woke up successfully, stream starting...', 'success');
  }

  private handleWakeupTimeout(): void {
    console.error('Wake-up failed: timeout reached');
    this.clearWakeupRetry();
    this.loading.set(false);
    this.currentCamera.set(null);
    this.setStatus(
      'Wake-up failed: Camera did not respond within timeout. Cannot stream.',
      'error'
    );
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
    return (
      this.videoInitPromise ||
      Promise.reject(new Error('Video initialization not started'))
    );
  }

  private setupVideoObserver(): void {
    if (!isPlatformBrowser(this.platformId)) return;

    this.videoObserver = new MutationObserver(() => {
      if (!this.videoElementReady() && this.videoElementRef?.nativeElement) {
        console.log('MutationObserver detected video element');
        this.attemptVideoInitialization();
      }
    });

    const hostElement = document.querySelector('app-video');
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
    console.log(
      `Video initialization attempt ${this.initializationAttempts}/${this.maxInitAttempts}`
    );

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

    if (typeof nativeElement.play !== 'function') {
      console.error('Video element missing play method');
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
      videoElement.muted = false;
      videoElement.controls = false;

      this.setupVideoEventListeners(videoElement);

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

  private setupVideoEventListeners(videoElement: HTMLVideoElement): void {
    const newVideoElement = videoElement.cloneNode(true) as HTMLVideoElement;
    videoElement.parentNode?.replaceChild(newVideoElement, videoElement);
    this.videoElement = newVideoElement;

    newVideoElement.addEventListener('error', (e) => {
      console.error('Video element error:', e);
      this.setStatus('Video playback error', 'error');
    });

    newVideoElement.addEventListener('loadstart', () => {
      console.log('Video loading started');
    });

    newVideoElement.addEventListener('loadeddata', () => {
      console.log('Video data loaded successfully');
    });

    newVideoElement.addEventListener('playing', () => {
      console.log('Video is playing');
    });

    newVideoElement.addEventListener('waiting', () => {
      console.log('Video is buffering');
    });

    newVideoElement.addEventListener('canplay', () => {
      console.log('Video can start playing');
    });
  }

  private scheduleNextAttempt(): void {
    if (this.initializationAttempts >= this.maxInitAttempts) {
      console.error(' Failed to initialize video element after maximum attempts');
      this.setStatus('Video element initialization failed', 'error');
      return;
    }

    const delay = Math.min(50 * Math.pow(2, this.initializationAttempts - 1), 2000);
    console.log(`⏱ Scheduling next attempt in ${delay}ms`);

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

  private resetVideoElement(): void {
    const video = this.getVideoElement();
    if (!video) return;

    try {
      video.pause();
      video.srcObject = null;
      video.removeAttribute('src');
      video.load();
      console.log('Video element reset to clean state');
    } catch (error) {
      console.warn('Error resetting video element:', error);
    }
  }

  // ============================================================================
  // MODE SWITCHING
  // ============================================================================

  setMode(mode: 'stream' | 'playback'): void {
    if (this.connected() || this.streaming()) {
      this.setStatus('Please disconnect before changing mode', 'warning');
      return;
    }

    this.mode.set(mode);
    if (isPlatformBrowser(this.platformId)) {
      localStorage.setItem('test_mode', mode);
    }

    this.currentCamera.set(null);
    this.playbackDate.set('');
    this.playbackTime.set('');
    this.statusMessage.set('');
  }

  // ============================================================================
  // PLAYBACK DATE/TIME FORMATTING
  // ============================================================================

  private formatPlaybackDateTime(): string {
    const dateStr = this.playbackDate();
    const timeStr = this.playbackTime();

    if (!dateStr || !timeStr) {
      throw new Error('Date and time are required for playback');
    }

    const dateParts = dateStr.split('-');
    if (dateParts.length !== 3) {
      throw new Error('Invalid date format. Expected YYYY-MM-DD');
    }

    const year = dateParts[0];
    const month = dateParts[1].padStart(2, '0');
    const day = dateParts[2].padStart(2, '0');

    const timeParts = timeStr.split(':');
    if (timeParts.length !== 2) {
      throw new Error('Invalid time format. Expected HH:MM');
    }

    let hour = parseInt(timeParts[0], 10);
    const minutes = timeParts[1].padStart(2, '0');

    hour = hour - 1;
    if (hour < 0) {
      hour = 23;
    }

    const formattedHour = hour.toString().padStart(2, '0');

    return `${year}${month}${day}-${formattedHour}${minutes}`;
  }

  private buildPlaybackInfos(camera: 'driver' | 'front' | 'rear'): string {
    const cameraId = DEFAULT_CONFIG.playbackCameras[camera];
    const formattedDateTime = this.formatPlaybackDateTime();
    return `PNormal/${cameraId}/${cameraId}REC${formattedDateTime}`;
  }

  // ============================================================================
  // STREAMING/PLAYBACK CONTROL
  // ============================================================================

  public async startStream(camera: 'driver' | 'front' | 'rear'): Promise<void> {
    if (!this.videoElementReady()) {
      this.setStatus('Initializing video element...', 'info');
      try {
        await this.waitForVideoElement();
      } catch (error) {
        this.setStatus('Video element not ready. Please refresh the page.', 'error');
        return;
      }
    }

    if (!this.connected()) {
      this.setStatus('Not connected to server', 'error');
      return;
    }

    if (this.mode() === 'playback' && !this.canStartPlayback()) {
      this.setStatus('Please enter date and time for playback', 'warning');
      return;
    }

    this.loading.set(true);
    this.currentCamera.set(camera);

    const isPlayback = this.mode() === 'playback';
    const cameraId = isPlayback
      ? DEFAULT_CONFIG.playbackCameras[camera]
      : DEFAULT_CONFIG.streamCameras[camera];

    console.log(`Starting ${isPlayback ? 'playback' : 'stream'} for camera:`, camera, cameraId);

    const cameraNames = {
      driver: 'driver camera',
      front: 'front camera',
      rear: 'rear camera'
    };

    const modeStr = isPlayback ? 'playback' : 'stream';
    this.setStatus(`Switching to ${cameraNames[camera]} (${modeStr})`, 'info');

    try {
      if (!this.peerConnection || this.peerConnection.connectionState === 'closed') {
        this.createPeerConnection();
      }

      let infos: string;

      if (isPlayback) {
        try {
          infos = this.buildPlaybackInfos(camera);
          console.log('Playback infos:', infos);
        } catch (error: any) {
          this.setStatus(error.message, 'error');
          this.loading.set(false);
          return;
        }
      } else {
        infos = cameraId;
      }

      if (!this.streaming()) {
        this.setStatus(`Requesting ${modeStr}...`, 'info');
        this.sendMessage({
          event: 'message',
          data: {
            source: this.myUsername,
            type: 'video-request',
            infos: infos,
            target: this.targetStreamId()
          }
        });
      } else {
        this.sendMessage({
          event: 'message',
          data: {
            source: this.myUsername,
            type: 'switch-cam',
            infos: infos,
            target: this.targetStreamId()
          }
        });
      }
    } catch (error) {
      console.error('Error starting stream:', error);
      this.loading.set(false);
      this.setStatus('Failed to start stream', 'error');
    }
  }

  // ============================================================================
  // TRACK EVENT HANDLER
  // ============================================================================

  private handleTrackEvent = (event: RTCTrackEvent): void => {
    console.log('Track event received');

    if (this.isWakingUp()) {
      this.handleWakeupSuccess();
    }

    if (!event.streams || event.streams.length === 0) {
      console.error('No streams in track event');
      this.setStatus('No video stream received', 'error');
      return;
    }

    const videoElement = this.getVideoElement();
    if (!videoElement) {
      console.error('Video element not available for track event');
      this.setStatus('Video element not available', 'error');
      return;
    }

    const newStream = event.streams[0];
    const wasReconnecting = this.isReconnectingSfu();

    this.remoteStream = newStream;

    // ── CAS RECONNEXION SFU - Utilisation du stream de secours ──────────────
    if (wasReconnecting && this.backupSfuReady() && this.backupStream) {
      console.log(' Using backup stream for SFU reconnect (atomic switch)');
      this.atomicSwitchToBackup();
      this.clearSfuReconnect();
      this.setStatus('SFU reconnected instantly (backup stream)', 'success');
      return;
    }

    // ── CAS RECONNEXION SFU standard (fallback) ────────────────────────────
    if (wasReconnecting) {
      console.log(' SFU reconnect: replacing srcObject with new stream');
      this.clearSfuReconnect();

      try {
        videoElement.srcObject = newStream;
        const playPromise = videoElement.play();
        playPromise?.catch(e => console.warn('Autoplay prevented on SFU reconnect:', e));
      } catch (error) {
        console.error('Error replacing srcObject on SFU reconnect:', error);
      }

      const modeStr = this.mode() === 'playback' ? 'Playback' : 'Stream';
      const sfuLabel = this.isSfuRelay() ? ' — SFU Relay active' : '';
      this.setStatus(`${modeStr} reconnected${sfuLabel}`, 'success');
      console.log(' Stream restored after SFU failover — no interruption');
      return;
    }

    // ── CAS NORMAL : première connexion ─────────────────────────────────────
    console.log(' Stream received — ready to relay to viewers if SFU role');

    try {
      videoElement.srcObject = newStream;

      const onLoadedData = () => {
        console.log('Video data loaded');
        this.streaming.set(true);
        this.loading.set(false);
        const modeStr = this.mode() === 'playback' ? 'Playback' : 'Stream';
        const sfuLabel = this.isSfuRelay() ? ' — SFU Relay active' : '';
        this.setStatus(`${modeStr} connected${sfuLabel}`, 'success');
        videoElement.removeEventListener('loadeddata', onLoadedData);
        videoElement.removeEventListener('error', onError);
      };

      const onError = (e: Event) => {
        console.error('Video element error:', e);
        this.setStatus('Video playback error', 'error');
        videoElement.removeEventListener('loadeddata', onLoadedData);
        videoElement.removeEventListener('error', onError);
      };

      videoElement.addEventListener('loadeddata', onLoadedData);
      videoElement.addEventListener('error', onError);

      const playPromise = videoElement.play();
      if (playPromise !== undefined) {
        playPromise.catch(error => {
          console.warn('Autoplay was prevented:', error);
          this.setStatus('Click video to play (autoplay blocked)', 'warning');
        });
      }
    } catch (error) {
      console.error('Error handling track event:', error);
      this.setStatus('Failed to display video stream', 'error');
    }
  };

  // ============================================================================
  // SFU FAILOVER — ULTRA RAPIDE AVEC DOUBLE-BUFFERING
  // ============================================================================

  private handleSfuDisconnected(reason: string): void {
    console.warn(` SFU disconnected (${reason}) — initiating ultra-fast failover`);

    const attempt = this.reconnectAttempts() + 1;
    this.reconnectAttempts.set(attempt);

    if (attempt > DEFAULT_CONFIG.sfuReconnectMaxAttempts) {
      console.error(' Max SFU reconnect attempts reached — falling back to full hangup');
      this.clearSfuReconnect();
      this.handleHangup();
      this.setStatus('Reconnection failed after multiple attempts — stream stopped', 'error');
      return;
    }

    const lostSfuId = this.currentSfuId;

    // ── SWITCH ATOMIQUE VERS LE STREAM DE SECOURS ──────────────────────────
    if (this.backupSfuReady() && this.backupStream) {
      console.log(' ATOMIC FAILOVER! Switching to backup stream immediately');
      const success = this.atomicSwitchToBackup();

      if (success) {
        this.setStatus('SFU switched instantly (atomic)', 'success');
        this.clearSfuReconnect();

        if (lostSfuId) {
          this.sendMessage({
            event: 'message',
            data: {
              source: this.myUsername,
              target: 'server',
              type: 'sfu-atomic-switch',
              oldSfuId: lostSfuId,
              newSfuId: this.currentSfuId
            }
          });
        }
        return;
      }
    }

    // Fallback : failover standard (si le backup n'est pas prêt)
    console.warn(' Backup not ready, using standard failover');
    this.currentSfuId = null;
    this.isReconnectingSfu.set(true);

    this.closePeerConnectionSilently();

    this.setStatus(
      `SFU connection lost — reconnecting... (attempt ${attempt}/${DEFAULT_CONFIG.sfuReconnectMaxAttempts})`,
      'warning'
    );

    if (this.connected()) {
      this.sendMessage({
        event: 'message',
        data: {
          source: this.myUsername,
          target: 'server',
          type: 'sfu-disconnected',
          lostSfuId: lostSfuId,
          cameraTarget: this.targetStreamId()
        }
      });
      console.log(` sfu-disconnected sent to server (lost SFU: ${lostSfuId})`);
    }

    // Timeout TRÈS COURT (500ms) pour le fallback
    this.sfuReconnectTimer = setTimeout(() => {
      if (this.isReconnectingSfu()) {
        console.warn(` No SFU offer received within ${DEFAULT_CONFIG.sfuReconnectTimeoutMs}ms — retrying video-request`);
        this.retryAfterSfuLoss();
      }
    }, DEFAULT_CONFIG.sfuReconnectTimeoutMs);
  }

  private retryAfterSfuLoss(): void {
    console.log(' retryAfterSfuLoss — sending video-request');

    if (this.sfuReconnectTimer) {
      clearTimeout(this.sfuReconnectTimer);
      this.sfuReconnectTimer = null;
    }

    const camera = this.currentCamera();
    if (!camera || !this.connected()) {
      console.error('Cannot retry: no camera or no connection');
      this.clearSfuReconnect();
      this.handleHangup();
      this.setStatus('Reconnection failed — stream stopped', 'error');
      return;
    }

    const isPlayback = this.mode() === 'playback';
    let infos: string;

    try {
      infos = isPlayback
        ? this.buildPlaybackInfos(camera)
        : DEFAULT_CONFIG.streamCameras[camera];
    } catch (e: any) {
      console.error('Cannot build infos for retry:', e);
      this.clearSfuReconnect();
      this.handleHangup();
      this.setStatus('Reconnection failed — stream stopped', 'error');
      return;
    }

    this.createPeerConnection();

    this.sendMessage({
      event: 'message',
      data: {
        source: this.myUsername,
        type: 'video-request',
        infos: infos,
        target: this.targetStreamId()
      }
    });

    this.sfuReconnectTimer = setTimeout(() => {
      if (this.isReconnectingSfu()) {
        console.error(' SFU reconnect final timeout after retry');
        this.clearSfuReconnect();
        this.handleHangup();
        this.setStatus('Reconnection failed — stream stopped', 'error');
      }
    }, DEFAULT_CONFIG.sfuReconnectTimeoutMs);
  }

  private clearSfuReconnect(): void {
    if (this.sfuReconnectTimer) {
      clearTimeout(this.sfuReconnectTimer);
      this.sfuReconnectTimer = null;
    }
    this.isReconnectingSfu.set(false);
    this.reconnectAttempts.set(0);
    console.log(' SFU reconnect state cleared');
  }

  private closePeerConnectionSilently(): void {
    if (!this.peerConnection) return;

    console.log(' Closing peer connection silently (preserving video srcObject)');

    try {
      this.peerConnection.ontrack = null;
      this.peerConnection.onicecandidate = null;
      this.peerConnection.oniceconnectionstatechange = null;
      this.peerConnection.onsignalingstatechange = null;
      this.peerConnection.onnegotiationneeded = null;
      this.peerConnection.ondatachannel = null;
      this.peerConnection.onicegatheringstatechange = null;

      if (this.peerConnection.connectionState !== 'closed') {
        this.peerConnection.close();
      }
    } catch (e) {
      console.warn('Error during silent close of peer connection:', e);
    }

    this.peerConnection = null;
  }

  // ============================================================================
  // SFU ROLE MANAGEMENT
  // ============================================================================

  private handleBecomeSfu(data: any): void {
    console.log(' Designated as SFU for camera:', data.cameraTarget);
    this.isSfuRelay.set(true);
    this.isRelayMode = true;

    this.sendMessage({
      event: 'message',
      data: {
        source: this.myUsername,
        target: 'server',
        type: 'sfu-accept'
      }
    });

    this.setStatus('SFU role accepted — relaying stream to viewers', 'success');
    console.log(' sfu-accept sent to server');
  }

  private async handleNewViewer(data: any): Promise<void> {
    const viewerId = data.viewerId;
    console.log(`SFU: New viewer to serve → ${viewerId}`);

    if (!this.remoteStream) {
      console.warn('SFU: No stream available yet to forward to viewer', viewerId);
      return;
    }

    if (this.sfuPeerConnections.has(viewerId)) {
      console.log(`SFU: Connection already exists for viewer ${viewerId}, skipping`);
      return;
    }

    try {
      const pc = new RTCPeerConnection(this.getPeerConnectionConfig());

      this.remoteStream.getTracks().forEach(track => {
        pc.addTrack(track, this.remoteStream!);
        console.log(`SFU: Added ${track.kind} track for viewer ${viewerId}`);
      });

      pc.onicecandidate = (event) => {
        if (event.candidate) {
          console.log(`SFU: Sending ICE candidate to viewer ${viewerId}`);
          this.sendMessage({
            event: 'message',
            data: {
              source: this.myUsername,
              target: viewerId,
              type: 'new-ice-candidate',
              candidate: event.candidate
            }
          });
        }
      };

      pc.oniceconnectionstatechange = () => {
        console.log(`SFU: ICE state for viewer ${viewerId}: ${pc.iceConnectionState}`);
        if (['disconnected', 'failed', 'closed'].includes(pc.iceConnectionState)) {
          console.log(`SFU: Viewer ${viewerId} disconnected`);
          this.sfuPeerConnections.delete(viewerId);
          this.activeViewers.set(this.sfuPeerConnections.size);
          pc.close();
          this.setStatus(
            `SFU: ${this.sfuPeerConnections.size} viewer(s) connected`,
            'info'
          );
        }
      };

      const offer = await pc.createOffer({
        offerToReceiveAudio: false,
        offerToReceiveVideo: false
      });
      await pc.setLocalDescription(offer);

      this.sendMessage({
        event: 'message',
        data: {
          source: this.myUsername,
          target: viewerId,
          type: 'video-offer',
          sdp: pc.localDescription
        }
      });

      this.sfuPeerConnections.set(viewerId, pc);
      this.activeViewers.set(this.sfuPeerConnections.size);

      console.log(
        ` SFU: Offer sent to viewer ${viewerId}. Total viewers: ${this.sfuPeerConnections.size}`
      );
      this.setStatus(
        `SFU: Relaying to ${this.sfuPeerConnections.size} viewer(s)`,
        'success'
      );
    } catch (error) {
      console.error(`SFU: Error creating connection for viewer ${viewerId}:`, error);
      this.setStatus(`Failed to connect viewer ${viewerId}`, 'error');
    }
  }

  private handleRedirectToSfu(data: any): void {
    const sfuId = data.sfuId;
    console.log(` Redirected to SFU: ${sfuId}`);

    // ── DOUBLE-BUFFERING: Préchargement immédiat du SFU de secours ──────────
    if (DEFAULT_CONFIG.preloadBackupSfu && sfuId !== this.currentSfuId) {
      console.log(` DOUBLE-BUFFERING: Preloading backup SFU ${sfuId}`);
      this.preloadBackupSfu(sfuId, data.infos);
    }

    this.currentSfuId = sfuId;

    if (this.isReconnectingSfu()) {
      console.log(' Redirect received during SFU failover — clearing reconnect timer');
      if (this.sfuReconnectTimer) {
        clearTimeout(this.sfuReconnectTimer);
        this.sfuReconnectTimer = null;
      }
      if (!this.peerConnection || this.peerConnection.connectionState === 'closed') {
        this.createPeerConnection();
      }
      this.setStatus(`Connecting to new SFU relay ${sfuId}...`, 'info');
      return;
    }

    if (!this.peerConnection || this.peerConnection.connectionState === 'closed') {
      this.createPeerConnection();
    }

    this.setStatus(`Connecting to SFU relay ${sfuId}...`, 'info');
    console.log(` Ready to receive video-offer from SFU ${sfuId}`);
  }

  private refuseBackupSfu(data: any): void {
    console.warn(' Refusing backup SFU role — no stream available');
    this.sendMessage({
      event: 'message',
      data: {
        source: this.myUsername,
        target: 'server',
        type: 'sfu-failed'
      }
    });
  }

  // ============================================================================
  // handleBecomeBackupSfu — CORRIGÉ (FIX ÉCRAN NOIR)
  //
  // FLUX :
  //   1. Créer cameraPeerConnection dédiée
  //   2. Envoyer video-request à la caméra
  //   3. handleVideoOffer détecte backupSfuState.waitingForCameraOffer
  //      → traite l'offre sur cameraPeerConnection
  //   4. cameraPc.ontrack déclenché :
  //      a. Met à jour remoteStream
  //      b. *** AFFICHE le stream sur videoElement du backup SFU *** ← FIX
  //      c. Envoie backup-sfu-accept au serveur
  // ============================================================================

  private async handleBecomeBackupSfu(data: any): Promise<void> {
    console.log(' Designated as backup SFU for camera:', data.cameraTarget);

    if (!this.remoteStream) {
      console.warn('Backup SFU: No stream available — refusing role');
      this.refuseBackupSfu(data);
      return;
    }

    this.isSfuRelay.set(true);
    this.isRelayMode = true;
    this.currentSfuId = null;

    // Nettoyer un éventuel état précédent
    this.cleanupBackupSfuState();

    // Créer la peer connection dédiée pour la connexion caméra
    const cameraPc = new RTCPeerConnection(this.getPeerConnectionConfig());

    // Initialiser l'état backup SFU
    this.backupSfuState = {
      cameraTarget: data.cameraTarget,
      failedSfuId: data.failedSfuId,
      infos: data.infos || '',
      waitingForCameraOffer: true,
      cameraSessionReady: false,
      cameraPeerConnection: cameraPc,
      connectTimeoutTimer: null
    };

    // ── Handler ontrack : stream caméra reçu ────────────────────────────────
    // FIX ÉCRAN NOIR : on affiche le stream sur le videoElement du backup SFU
    // ET on met à jour remoteStream pour le relay vers les viewers.
    cameraPc.ontrack = (event) => {
      if (!this.backupSfuState) return;

      console.log(' Backup SFU: camera stream received via ontrack');

      if (event.streams && event.streams[0]) {
        // Mettre à jour remoteStream pour relay vers viewers
        this.remoteStream = event.streams[0];
        console.log(' Backup SFU: remoteStream updated with camera stream');

        // ════════════════════════════════════════════════════════════════════
        // FIX ÉCRAN NOIR : Afficher le stream sur le videoElement du backup SFU
        // Sans ce bloc, le backup SFU voit un écran noir alors que les viewers
        // reçoivent bien le flux (remoteStream est correct mais non affiché).
        // ════════════════════════════════════════════════════════════════════
        const videoElement = this.getVideoElement();
        if (videoElement) {
          try {
            videoElement.srcObject = this.remoteStream;

            const onLoadedData = () => {
              console.log(' Backup SFU: video displayed on local element');
              this.streaming.set(true);
              this.loading.set(false);
              const modeStr = this.mode() === 'playback' ? 'Playback' : 'Stream';
              this.setStatus(`${modeStr} connected — Backup SFU Relay active`, 'success');
              videoElement.removeEventListener('loadeddata', onLoadedData);
              videoElement.removeEventListener('error', onVideoError);
            };

            const onVideoError = (e: Event) => {
              console.error('Backup SFU: video element error after stream set:', e);
              this.setStatus('Backup SFU: video playback error', 'error');
              videoElement.removeEventListener('loadeddata', onLoadedData);
              videoElement.removeEventListener('error', onVideoError);
            };

            videoElement.addEventListener('loadeddata', onLoadedData);
            videoElement.addEventListener('error', onVideoError);

            const playPromise = videoElement.play();
            if (playPromise !== undefined) {
              playPromise.catch(error => {
                console.warn('Backup SFU: autoplay prevented:', error);
                this.setStatus('Backup SFU: Click video to play (autoplay blocked)', 'warning');
              });
            }
          } catch (error) {
            console.error('Backup SFU: error setting srcObject on videoElement:', error);
            this.setStatus('Backup SFU: Failed to display video stream', 'error');
          }
        } else {
          // Fallback si videoElement non disponible : on marque quand même comme streaming
          console.error('Backup SFU: videoElement not available — stream not displayed locally');
          this.streaming.set(true);
          this.loading.set(false);
        }
      }

      // Marquer la session comme prête
      this.backupSfuState.cameraSessionReady = true;
      this.backupSfuState.waitingForCameraOffer = false;

      // Annuler le timeout
      if (this.backupSfuState.connectTimeoutTimer) {
        clearTimeout(this.backupSfuState.connectTimeoutTimer);
        this.backupSfuState.connectTimeoutTimer = null;
      }

      // MAINTENANT seulement on notifie le serveur qu'on est prêt
      console.log(' Backup SFU: sending backup-sfu-accept (camera session established)');
      this.sendMessage({
        event: 'message',
        data: {
          source: this.myUsername,
          target: 'server',
          type: 'backup-sfu-accept'
        }
      });
    };

    // ── Handler onicecandidate : transmettre les ICE candidates à la caméra ─
    cameraPc.onicecandidate = (event) => {
      if (event.candidate && this.backupSfuState) {
        this.sendMessage({
          event: 'message',
          data: {
            source: this.myUsername,
            target: this.backupSfuState.cameraTarget,
            type: 'new-ice-candidate',
            candidate: event.candidate
          }
        });
      }
    };

    // ── Handler iceconnectionstatechange ────────────────────────────────────
    cameraPc.oniceconnectionstatechange = () => {
      const state = cameraPc.iceConnectionState;
      console.log(`📡 Backup SFU camera ICE state: ${state}`);

      if (state === 'connected') {
        console.log(' Backup SFU: ICE connected to camera');
      } else if (state === 'failed') {
        console.error(' Backup SFU: ICE connection to camera FAILED');
        this.cleanupBackupSfuState();
        this.refuseBackupSfu(data);
      }
    };

    // ── Timeout de sécurité ─────────────────────────────────────────────────
    this.backupSfuState.connectTimeoutTimer = setTimeout(() => {
      if (this.backupSfuState && !this.backupSfuState.cameraSessionReady) {
        console.error(
          ` Backup SFU: timeout waiting for camera session (${DEFAULT_CONFIG.backupSfuCameraConnectTimeoutMs}ms)`
        );
        this.cleanupBackupSfuState();
        this.refuseBackupSfu(data);
      }
    }, DEFAULT_CONFIG.backupSfuCameraConnectTimeoutMs);

    // ── Étape 1 : Envoyer video-request à la caméra ─────────────────────────
    console.log(` Backup SFU: sending video-request to camera ${data.cameraTarget}`);
    this.setStatus(`Backup SFU: connecting to camera ${data.cameraTarget}...`, 'info');

    this.sendMessage({
      event: 'message',
      data: {
        source: this.myUsername,
        type: 'video-request',
        infos: data.infos || '',
        target: data.cameraTarget
      }
    });

    // Le flux continue dans handleVideoOffer() qui détectera backupSfuState.waitingForCameraOffer
  }

  private cleanupSfuConnections(): void {
    console.log(`Cleaning up ${this.sfuPeerConnections.size} SFU connections`);
    this.sfuPeerConnections.forEach((pc, viewerId) => {
      try {
        pc.close();
      } catch (error) {
        console.warn(`Error closing connection for viewer ${viewerId}:`, error);
      }
    });
    this.sfuPeerConnections.clear();
    this.activeViewers.set(0);
    this.isSfuRelay.set(false);
    this.isRelayMode = false;
    this.remoteStream = null;
    this.currentSfuId = null;
    this.nextSfuId = null;
    this.clearSfuReconnect();
    this.cleanupBackupConnection();
    this.cleanupBackupSfuState();
  }

  // ============================================================================
  // HANGUP AND DISCONNECT
  // ============================================================================

  public hangup(): void {
    if (!this.streaming() && !this.isWakingUp() && !this.isReconnectingSfu()) return;

    console.log('Hanging up call');

    if (this.isWakingUp()) {
      this.clearWakeupRetry();
    }

    if (this.isReconnectingSfu()) {
      this.clearSfuReconnect();
    }

    try {
      if (this.socket$ && this.targetStreamId()) {
        this.sendMessage({
          event: 'message',
          data: {
            source: this.myUsername,
            target: this.targetStreamId(),
            type: 'hang-up'
          }
        });
      }
    } catch (error) {
      console.warn('Error sending hangup message:', error);
    }

    this.handleHangup();
    this.setStatus('Stream ended - still connected', 'info');
  }

  private handleHangup(): void {
    console.log('Handling hangup - resetting peer connection and SFU relay');

    this.streaming.set(false);
    this.currentCamera.set(null);
    this.loading.set(false);
    this.clearWakeupRetry();
    this.clearSfuReconnect();
    this.cleanupSfuConnections();
    this.cleanupBackupConnection();
    this.cleanupBackupSfuState();
    this.resetVideoElement();
    this.closeAndResetPeerConnection();
  }

  public disconnect(): void {
    console.log('Disconnecting from server - full cleanup');

    try {
      this.stopPing();
      this.clearWakeupRetry();
      this.clearSfuReconnect();
      this.cleanupSfuConnections();
      this.cleanupBackupConnection();
      this.cleanupBackupSfuState();

      if (this.streaming()) {
        try {
          if (this.socket$ && this.targetStreamId()) {
            this.sendMessage({
              event: 'message',
              data: {
                source: this.myUsername,
                target: this.targetStreamId(),
                type: 'hang-up'
              }
            });
          }
        } catch (error) {
          console.warn('Error sending hangup during disconnect:', error);
        }
      }

      if (this.socket$ && this.connected()) {
        try {
          this.sendMessage({
            event: 'message',
            data: {
              source: this.myUsername,
              target: this.targetStreamId() || 'server',
              type: 'disconnect'
            }
          });
          console.log('Sent disconnect message to server');
        } catch (error) {
          console.warn('Error sending disconnect message:', error);
        }
      }

      this.resetVideoElement();

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

      this.closeAndResetPeerConnection();

      this.connected.set(false);
      this.streaming.set(false);
      this.loading.set(false);
      this.currentCamera.set(null);

      this.messagesSubject$ = new Subject<any>();
      this.messages$ = this.messagesSubject$.asObservable();

      this.setStatus('Disconnected - Ready for new connection', 'info');
    } catch (error) {
      console.error('Error during disconnect:', error);
      this.socket$ = null;
      this.peerConnection = null;
      this.messageSubscription = null;
      this.connected.set(false);
      this.streaming.set(false);
      this.stopPing();
      this.clearWakeupRetry();
      this.clearSfuReconnect();
      this.cleanupSfuConnections();
      this.cleanupBackupConnection();
      this.cleanupBackupSfuState();
      this.setStatus('Disconnected (with errors)', 'warning');
    }
  }

  // ============================================================================
  // WEBSOCKET CONNECTION
  // ============================================================================

  public connect(): void {
    if (!this.canConnect()) {
      if (!this.websocketUrl().trim() || this.websocketUrl() === 'wss://') {
        this.setStatus('Please enter WebSocket URL', 'warning');
      } else if (!this.targetStreamId().trim()) {
        this.setStatus('Please enter Target Stream ID', 'warning');
      }
      return;
    }

    if (isPlatformBrowser(this.platformId)) {
      localStorage.setItem('test_ws_url', this.websocketUrl());
      localStorage.setItem('test_target_id', this.targetStreamId());
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
          this.clearWakeupRetry();
          this.clearSfuReconnect();
          this.setStatus(
            'Connection error: ' + (error.message || 'Unknown'),
            'error'
          );
        },
        () => {
          console.log('WebSocket connection closed');
          this.connected.set(false);
          this.loading.set(false);
          this.stopPing();
          this.clearWakeupRetry();
          this.clearSfuReconnect();
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
          this.clearWakeupRetry();
          this.clearSfuReconnect();
          this.setStatus(
            'Connection closed: ' + (event.reason || 'Unknown reason'),
            'warning'
          );
        }
      }
    });
  }

  private registerWithServer(): void {
    this.sendMessage({
      event: 'register-client',
      data: {
        id: this.myUsername,
        name: this.myUsername,
        date: Date.now(),
        type: 'pingv2',
        peerType: 'mobile'
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
      data: { name: this.myUsername }
    });
  }

  // ============================================================================
  // MESSAGE HANDLING
  // ============================================================================

  private setupMessageHandlers(): void {
    if (this.messageSubscription) {
      this.messageSubscription.unsubscribe();
    }

    console.log('Subscribing to incoming messages');
    this.messageSubscription = this.messages$
      .pipe(
        tap(msg => {
          if (msg.data != null) {
            const msgType = msg.data.type;
            console.log(`Received message of type: ${msgType}`);

            switch (msgType) {

              case 'hangup':
              case 'hang-up':
                this.handleHangupMessage();
                break;

              case 'ice-candidate':
              case 'new-ice-candidate':
                this.handleIceCandidate(msg.data.candidate, msg.data.source);
                break;

              case 'video-offer':
                this.handleVideoOffer(msg.data.sdp, msg.data.source);
                break;

              case 'video-answer':
                this.handleVideoAnswer(msg.data.sdp, msg.data.source);
                break;

              case 'video-response':
                console.log('Received video response');
                this.setStatus('Video request acknowledged', 'info');
                break;

              case 'wake-up':
              case 'wakeup':
                this.handleWakeupMessage(msg.data);
                break;

              case 'become-sfu':
                this.handleBecomeSfu(msg.data);
                break;

              case 'new-viewer':
                this.handleNewViewer(msg.data);
                break;

              case 'redirect-to-sfu':
                this.handleRedirectToSfu(msg.data);
                break;

              case 'become-backup-sfu':
                this.handleBecomeBackupSfu(msg.data);
                break;

              // ── Double-buffering SFU ─────────────────────────────────────
              case 'backup-preload-offer':
                this.handleBackupPreloadOffer(msg.data);
                break;

              case 'backup-preload-answer':
                this.handleBackupPreloadAnswer(msg.data);
                break;

              case 'error':
                console.log('Error message:', JSON.stringify(msg.data));
                this.setStatus(
                  'Server error: ' + (msg.data.message || 'Unknown error'),
                  'error'
                );
                break;

              default:
                console.log('Unhandled message type', msgType);
                break;
            }
          } else if (msg.event === 'pong') {
            console.log('Received PONG from server (pingv2)');
          } else if (msg.event === 'ping') {
            console.log('Received PING from server (legacy mode)');
            this.sendMessage({
              event: 'pong',
              data: { name: this.myUsername }
            });
          }
        })
      )
      .subscribe(
        () => {},
        (error) => {
          console.error('Error in message subscription:', error);
          this.setStatus('Connection error: Failed to process messages', 'error');
          this.stopPing();
          this.clearWakeupRetry();
          this.clearSfuReconnect();
        }
      );
  }

  private handleBackupPreloadOffer(data: any): void {
    if (!this.backupPeerConnection) {
      console.warn('No backup connection available for offer');
      return;
    }

    console.log(' Received backup preload offer from SFU');

    this.backupPeerConnection.setRemoteDescription(new RTCSessionDescription(data.sdp))
      .then(async () => {
        const answer = await this.backupPeerConnection!.createAnswer();
        await this.backupPeerConnection!.setLocalDescription(answer);

        this.sendMessage({
          event: 'message',
          data: {
            source: this.myUsername,
            target: data.source,
            type: 'backup-preload-answer',
            sdp: answer
          }
        });

        console.log(' Backup preload answer sent');
      })
      .catch(error => {
        console.error('Error handling backup preload offer:', error);
        this.cleanupBackupConnection();
      });
  }

  private handleBackupPreloadAnswer(data: any): void {
    if (!this.backupPeerConnection) {
      console.warn('No backup connection for answer');
      return;
    }

    console.log(' Received backup preload answer');
    this.backupPeerConnection.setRemoteDescription(new RTCSessionDescription(data.sdp))
      .catch(error => {
        console.error('Error handling backup preload answer:', error);
        this.cleanupBackupConnection();
      });
  }

  private handleWakeupMessage(data: any): void {
    console.log('Wake up message received:', data);
    const message = data.message || 'Camera is waking up...';

    if (!this.isWakingUp() && this.currentCamera()) {
      const camera = this.currentCamera()!;
      const isPlayback = this.mode() === 'playback';

      let infos: string;
      if (isPlayback) {
        try {
          infos = this.buildPlaybackInfos(camera);
        } catch (error: any) {
          this.setStatus(error.message, 'error');
          this.loading.set(false);
          return;
        }
      } else {
        infos = DEFAULT_CONFIG.streamCameras[camera];
      }

      this.setStatus(message + ' (Retrying...)', 'info');
      this.startWakeupRetry(camera, infos, isPlayback);
    } else if (this.isWakingUp()) {
      const remaining = this.wakeupTimeRemaining();
      this.setStatus(`${message} (${remaining}s remaining)`, 'info');
    }
  }

  private handleHangupMessage(): void {
    console.log('Received hangup message from peer');
    if (this.isReconnectingSfu()) {
      this.clearSfuReconnect();
    }
    this.handleHangup();
    this.setStatus('Call ended by remote peer', 'info');
  }

  // ============================================================================
  // WEBRTC PEER CONNECTION
  // ============================================================================

  private createPeerConnection(): void {
    this.peerConnection = new RTCPeerConnection(this.getPeerConnectionConfig());

    this.peerConnection.onicecandidate = this.handleICECandidateEvent;
    this.peerConnection.oniceconnectionstatechange = this.handleICEConnectionStateChangeEvent;
    this.peerConnection.onicegatheringstatechange = this.handleICEGatheringStateChangeEvent;
    this.peerConnection.onsignalingstatechange = this.handleSignalingStateChangeEvent;
    this.peerConnection.ondatachannel = this.handleDataChannel;
    this.peerConnection.ontrack = this.handleTrackEvent;
    this.peerConnection.onnegotiationneeded = this.handleNegotiationNeededEvent.bind(this);

    console.log('Peer connection created with config:', this.getPeerConnectionConfig());
  }

  private handleICECandidateEvent = (event: RTCPeerConnectionIceEvent): void => {
    if (event.candidate) {
      console.log('Outgoing ICE candidate:', JSON.stringify(event.candidate, null, 2));

      const target = this.currentSfuId || this.targetStreamId();

      this.sendMessage({
        event: 'message',
        data: {
          candidate: event.candidate,
          source: this.myUsername,
          target: target,
          type: 'new-ice-candidate'
        }
      });
    }
  };

  private handleICEConnectionStateChangeEvent = (): void => {
    if (!this.peerConnection) return;
    const state = this.peerConnection.iceConnectionState;
    console.log('ICE connection state changed to:', state);

    switch (state) {
      case 'closed':
        if (!this.isReconnectingSfu()) {
          this.setStatus('Connection closed', 'info');
        }
        break;

      case 'failed':
      case 'disconnected':
        if (this.currentSfuId && !this.isReconnectingSfu()) {
          console.log(` SFU connection lost via ICE ${state} — starting seamless failover`);
          this.handleSfuDisconnected(`ice-${state}`);
        } else if (!this.isReconnectingSfu()) {
          this.handleHangup();
          this.setStatus('Connection disconnected', 'warning');
        }
        break;

      case 'connected':
        if (this.isReconnectingSfu()) {
          console.log(' ICE connected after SFU failover — waiting for track');
          this.setStatus('SFU reconnected — restoring stream...', 'success');
        } else {
          this.setStatus('Network connection established', 'success');
        }
        break;

      case 'checking':
        if (this.isReconnectingSfu()) {
          this.setStatus('Checking new SFU connection...', 'info');
        }
        break;
    }
  };

  private handleICEGatheringStateChangeEvent = (): void => {
    if (!this.peerConnection) return;
    console.log('ICE gathering state changed to:', this.peerConnection.iceGatheringState);
  };

  private handleSignalingStateChangeEvent = (): void => {
    if (!this.peerConnection) return;
    console.log('WebRTC signaling state changed to:', this.peerConnection.signalingState);

    if (this.peerConnection.signalingState === 'closed') {
      if (!this.isReconnectingSfu()) {
        this.setStatus('Connection closed', 'info');
      }
    }
  };

  private handleDataChannel = (): void => {
    console.log('Data channel event received');
  };

  private async handleNegotiationNeededEvent(): Promise<void> {
    console.log('Negotiation needed');

    if (!this.peerConnection) return;

    try {
      if (this.peerConnection.signalingState !== 'stable') {
        console.log('Connection not stable, postponing negotiation');
        return;
      }

      console.log('Creating offer');
      const offer = await this.peerConnection.createOffer(DEFAULT_CONFIG.offerOptions);

      console.log('Setting local description');
      await this.peerConnection.setLocalDescription(offer);

      console.log('Sending offer to remote peer');
      this.sendMessage({
        event: 'message',
        data: {
          source: this.myUsername,
          target: this.targetStreamId(),
          type: 'video-offer',
          sdp: this.peerConnection.localDescription
        }
      });
    } catch (error) {
      console.error('Error during negotiation:', error);
      this.setStatus('Connection negotiation failed', 'error');
    }
  }

  // ============================================================================
  // handleVideoOffer — routing backup SFU vs normal
  //
  // Si backupSfuState.waitingForCameraOffer est true et que l'offre vient de la
  // caméra cible, on la route vers cameraPeerConnection au lieu de peerConnection.
  // ============================================================================

  private async handleVideoOffer(
    sdp: RTCSessionDescriptionInit,
    sourceId?: string
  ): Promise<void> {
    if (sourceId && this.sfuPeerConnections.has(sourceId)) {
      console.log(`SFU: Received unexpected offer from viewer ${sourceId} — ignoring`);
      return;
    }

    // ── CAS BACKUP SFU : offre de la caméra pour le processus failover ──────
    // Debug log pour diagnostiquer les problèmes de routing
    console.log(`🔍 handleVideoOffer: sourceId=${sourceId}, backupSfuState=${JSON.stringify({
      waitingForCameraOffer: this.backupSfuState?.waitingForCameraOffer,
      cameraTarget: this.backupSfuState?.cameraTarget
    })}`);

    if (
      this.backupSfuState?.waitingForCameraOffer &&
      this.backupSfuState.cameraPeerConnection &&
      sourceId === this.backupSfuState.cameraTarget
    ) {
      console.log(` Backup SFU: received video-offer from camera ${sourceId} — processing on cameraPeerConnection`);

      try {
        const cameraPc = this.backupSfuState.cameraPeerConnection;
        await cameraPc.setRemoteDescription(new RTCSessionDescription(sdp));

        const answer = await cameraPc.createAnswer();
        await cameraPc.setLocalDescription(answer);

        console.log(`📤 Backup SFU: sending video-answer to camera ${sourceId}`);
        this.sendMessage({
          event: 'message',
          data: {
            source: this.myUsername,
            target: sourceId,
            type: 'video-answer',
            sdp: cameraPc.localDescription
          }
        });

        // On attend maintenant ontrack sur cameraPc avant d'envoyer backup-sfu-accept
      } catch (error) {
        console.error(' Backup SFU: error handling camera video-offer:', error);
        this.cleanupBackupSfuState();
        this.refuseBackupSfu({});
      }
      return;
    }

    // ── CAS NORMAL ───────────────────────────────────────────────────────────
    if (!this.peerConnection || this.peerConnection.signalingState === 'closed') {
      this.createPeerConnection();
    }

    try {
      const desc = new RTCSessionDescription(sdp);
      const offerSource = sourceId || this.targetStreamId();
      const isFailover = this.isReconnectingSfu();

      console.log(`Received video-offer from: ${offerSource}${isFailover ? ' [FAILOVER]' : ''}`);

      if (sourceId && sourceId !== this.targetStreamId()) {
        this.currentSfuId = sourceId;
        console.log(`Updated currentSfuId to: ${sourceId}`);
      }

      await this.peerConnection!.setRemoteDescription(desc);
      console.log('Creating answer');

      const answer = await this.peerConnection!.createAnswer();
      await this.peerConnection!.setLocalDescription(answer);

      console.log(`Sending video-answer to: ${offerSource}`);
      this.sendMessage({
        event: 'message',
        data: {
          source: this.myUsername,
          target: offerSource,
          type: 'video-answer',
          sdp: this.peerConnection!.localDescription
        }
      });

      if (isFailover) {
        if (this.sfuReconnectTimer) {
          clearTimeout(this.sfuReconnectTimer);
          this.sfuReconnectTimer = null;
        }
        this.setStatus('New SFU offer received — waiting for stream...', 'info');
      }
    } catch (error) {
      console.error('Error handling video offer:', error);
      if (this.isReconnectingSfu()) {
        console.warn('Error during failover offer — scheduling retry');
        this.sfuReconnectTimer = setTimeout(() => {
          if (this.isReconnectingSfu()) {
            this.retryAfterSfuLoss();
          }
        }, 500);
      } else {
        this.setStatus('Failed to process video offer', 'error');
      }
    }
  }

  private async handleVideoAnswer(
    sdp: RTCSessionDescriptionInit,
    sourceId?: string
  ): Promise<void> {
    if (sourceId && this.sfuPeerConnections.has(sourceId)) {
      const pc = this.sfuPeerConnections.get(sourceId)!;
      await pc.setRemoteDescription(new RTCSessionDescription(sdp));
      console.log(` SFU: Viewer ${sourceId} connected successfully`);
      return;
    }

    if (!this.peerConnection) return;
    await this.peerConnection.setRemoteDescription(new RTCSessionDescription(sdp));
    console.log('Video answer set — connection established');

    if (!this.isReconnectingSfu()) {
      this.setStatus('Connection established', 'success');
    }
  }

  // ============================================================================
  // handleIceCandidate — routing backup SFU vs normal
  //
  // Si l'ICE candidate vient de la caméra (sourceId === backupSfuState.cameraTarget)
  // et qu'on est en mode backup SFU, on le route vers cameraPeerConnection.
  // ============================================================================

  private async handleIceCandidate(
    candidate: RTCIceCandidateInit,
    sourceId?: string
  ): Promise<void> {
    // ── CAS BACKUP SFU : ICE candidate de la caméra → cameraPeerConnection ──
    if (
      sourceId &&
      this.backupSfuState?.cameraPeerConnection &&
      sourceId === this.backupSfuState.cameraTarget
    ) {
      const cameraPc = this.backupSfuState.cameraPeerConnection;
      try {
        if (cameraPc.remoteDescription) {
          await cameraPc.addIceCandidate(new RTCIceCandidate(candidate));
          console.log(`✅ Backup SFU: added ICE candidate from camera ${sourceId}`);
        } else {
          console.warn('Backup SFU: ignoring ICE candidate — no remote description yet on cameraPc');
        }
      } catch (e) {
        console.warn('Backup SFU: error adding ICE candidate from camera:', e);
      }
      return;
    }

    // Si c'est un candidate pour la connexion de secours (double-buffering)
    if (sourceId && this.backupSfuId === sourceId && this.backupPeerConnection) {
      if (this.backupPeerConnection.remoteDescription) {
        this.backupPeerConnection.addIceCandidate(new RTCIceCandidate(candidate))
          .catch(e => console.warn('Error adding ICE candidate to backup:', e));
      } else {
        this.pendingBackupIceCandidates.push(candidate);
      }
      return;
    }

    if (sourceId && this.sfuPeerConnections.has(sourceId)) {
      try {
        await this.sfuPeerConnections
          .get(sourceId)!
          .addIceCandidate(new RTCIceCandidate(candidate));
        console.log(`SFU: Added ICE candidate for viewer ${sourceId}`);
      } catch (error) {
        console.warn(`SFU: Error adding ICE candidate for viewer ${sourceId}:`, error);
      }
      return;
    }

    if (!this.peerConnection) return;
    try {
      await this.peerConnection.addIceCandidate(new RTCIceCandidate(candidate));
      console.log('Added ICE candidate from', sourceId || 'camera');
    } catch (error) {
      console.warn('Error adding ICE candidate:', error);
    }
  }

  private closeAndResetPeerConnection(): void {
    if (!this.peerConnection) {
      console.log('No peer connection to close');
      this.createPeerConnection();
      return;
    }

    try {
      if (
        this.peerConnection.connectionState === 'closed' ||
        this.peerConnection.signalingState === 'closed'
      ) {
        console.log('Peer connection already closed');
        this.peerConnection = null;
        this.createPeerConnection();
        return;
      }

      this.peerConnection.ontrack = null;
      this.peerConnection.onicecandidate = null;
      this.peerConnection.oniceconnectionstatechange = null;
      this.peerConnection.onsignalingstatechange = null;
      this.peerConnection.onnegotiationneeded = null;
      this.peerConnection.ondatachannel = null;
      this.peerConnection.onicegatheringstatechange = null;

      try {
        const transceivers = this.peerConnection.getTransceivers();
        transceivers.forEach((transceiver, index) => {
          try {
            if (transceiver.receiver?.track) transceiver.receiver.track.stop();
            if (transceiver.sender?.track) transceiver.sender.track.stop();
            if (this.peerConnection!.connectionState !== 'closed') transceiver.stop();
          } catch (error) {
            console.warn(`Error stopping transceiver ${index}:`, error);
          }
        });
      } catch (error) {
        console.warn('Error accessing transceivers:', error);
      }

      try {
        this.peerConnection.close();
      } catch (error) {
        console.warn('Error closing peer connection:', error);
      }

      this.peerConnection = null;
    } catch (error) {
      console.error('Error during peer connection cleanup:', error);
      this.peerConnection = null;
    } finally {
      this.createPeerConnection();
      console.log('Peer connection reset complete');
    }
  }

  // ============================================================================
  // CLEANUP METHODS
  // ============================================================================

  private performSafeCleanup(): void {
    console.log('Safe cleanup triggered');
    try {
      if (this.streaming() && this.targetStreamId()) {
        this.sendMessage({
          event: 'message',
          data: {
            source: this.myUsername,
            target: this.targetStreamId(),
            type: 'hang-up'
          }
        });
      }
      this.cleanup();
    } catch (error) {
      console.error('Error during safe cleanup:', error);
    }
  }

  private performEmergencyCleanup(): void {
    console.log('Emergency cleanup triggered');
    try {
      if (this.streaming() && this.targetStreamId()) {
        try {
          this.sendMessage({
            event: 'message',
            data: {
              source: this.myUsername,
              target: this.targetStreamId(),
              type: 'hang-up'
            }
          });
        } catch (error) {
          console.warn('Error sending hangup message:', error);
        }
      }

      if (this.peerConnection && this.peerConnection.connectionState !== 'closed') {
        try {
          this.peerConnection.getTransceivers().forEach(transceiver => {
            if (transceiver.receiver?.track) transceiver.receiver.track.stop();
            if (transceiver.sender?.track) transceiver.sender.track.stop();
          });
        } catch (e) {
          console.warn('Error stopping transceivers:', e);
        }

        try {
          this.peerConnection.close();
        } catch (e) {
          console.warn('Error closing peer connection:', e);
        }
      }

      this.cleanupBackupConnection();
      this.cleanupBackupSfuState();

      if (this.socket$) {
        try {
          this.socket$.complete();
        } catch (e) {
          console.warn('Error completing socket:', e);
        }
      }

      this.cleanup();
    } catch (error) {
      console.error('Error during emergency cleanup:', error);
    }
  }

  private cleanup(): void {
    try {
      this.stopPing();
      this.clearWakeupRetry();
      this.clearSfuReconnect();
      this.cleanupSfuConnections();
      this.cleanupBackupConnection();
      this.cleanupBackupSfuState();

      if (this.streaming()) {
        this.hangup();
      }

      this.resetVideoElement();

      if (this.messageSubscription) {
        this.messageSubscription.unsubscribe();
        this.messageSubscription = null;
      }

      if (this.socket$) {
        this.socket$.complete();
        this.socket$ = null;
      }

      if (this.peerConnection) {
        this.peerConnection.close();
        this.peerConnection = null;
      }

      this.connected.set(false);
      this.streaming.set(false);
      this.loading.set(false);
      this.currentCamera.set(null);
    } catch (error) {
      console.error('Error during cleanup:', error);
    }
  }

  // ============================================================================
  // UTILITY METHODS
  // ============================================================================

  private getBrowserId(): string {
    if (!isPlatformBrowser(this.platformId)) {
      return uuidv4();
    }

    let browserId = localStorage.getItem('browser_id_test');
    if (!browserId) {
      browserId = uuidv4();
      localStorage.setItem('browser_id_test', browserId);
    }
    return browserId;
  }

  private setStatus(
    message: string,
    type: 'success' | 'info' | 'warning' | 'error'
  ): void {
    this.statusMessage.set(message);
    this.statusType.set(type);
    console.log(`[${type.toUpperCase()}] ${message}`);
  }

  onWebsocketUrlChange(value: string): void {
    this.websocketUrl.set(value);
    this.statusMessage.set('');
  }

  onTargetStreamIdChange(value: string): void {
    this.targetStreamId.set(value);
    this.statusMessage.set('');
  }

  onPlaybackDateChange(value: string): void {
    this.playbackDate.set(value);
    this.statusMessage.set('');
  }

  onPlaybackTimeChange(value: string): void {
    this.playbackTime.set(value);
    this.statusMessage.set('');
  }
}