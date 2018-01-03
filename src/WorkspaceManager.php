<?php

namespace Drupal\workspace;

use Drupal\Core\DependencyInjection\ClassResolverInterface;
use Drupal\Core\Entity\EntityPublishedInterface;
use Drupal\Core\Entity\EntityTypeInterface;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Session\AccountProxyInterface;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use Psr\Log\LoggerInterface;
use Symfony\Component\HttpFoundation\RequestStack;

/**
 * Provides the workspace manager.
 */
class WorkspaceManager implements WorkspaceManagerInterface {

  use StringTranslationTrait;

  /**
   * The default workspace ID.
   */
  const DEFAULT_WORKSPACE = 'live';

  /**
   * An array of entity type IDs that can not belong to a workspace.
   *
   * By default, only entity types which are revisionable and publishable can
   * belong to a workspace.
   *
   * @var string[]
   */
  protected $blacklist = [
    'workspace_association',
    'replication_log',
    'workspace',
  ];

  /**
   * The request stack.
   *
   * @var \Symfony\Component\HttpFoundation\RequestStack
   */
  protected $requestStack;

  /**
   * The entity type manager.
   *
   * @var \Drupal\Core\Entity\EntityTypeManagerInterface
   */
  protected $entityTypeManager;

  /**
   * The current user.
   *
   * @var \Drupal\Core\Session\AccountProxyInterface
   */
  protected $currentUser;

  /**
   * A list of workspace negotiators.
   *
   * @var \Drupal\workspace\Negotiator\WorkspaceNegotiatorInterface[]
   */
  protected $negotiators = [];

  /**
   * A list of workspace negotiators sorted by their priority.
   *
   * @var \Drupal\workspace\Negotiator\WorkspaceNegotiatorInterface[]
   */
  protected $sortedNegotiators;

  /**
   * A logger instance.
   *
   * @var \Psr\Log\LoggerInterface
   */
  protected $logger;

  /**
   * The class resolver.
   *
   * @var \Drupal\Core\DependencyInjection\ClassResolverInterface
   */
  protected $classResolver;

  /**
   * The workspace negotiator service IDs.
   *
   * @var array
   */
  protected $negotiatorIds;

  /**
   * Constructs a new WorkspaceManager.
   *
   * @param \Symfony\Component\HttpFoundation\RequestStack $request_stack
   *   The request stack.
   * @param \Drupal\Core\Entity\EntityTypeManagerInterface $entity_type_manager
   *   The entity type manager.
   * @param \Drupal\Core\Session\AccountProxyInterface $current_user
   *   The current user.
   * @param \Psr\Log\LoggerInterface $logger
   *   A logger instance.
   * @param \Drupal\Core\DependencyInjection\ClassResolverInterface $class_resolver
   *   The class resolver.
   * @param array $negotiator_ids
   *   The workspace negotiator service IDs.
   */
  public function __construct(RequestStack $request_stack, EntityTypeManagerInterface $entity_type_manager, AccountProxyInterface $current_user, LoggerInterface $logger, ClassResolverInterface $class_resolver, array $negotiator_ids) {
    $this->requestStack = $request_stack;
    $this->entityTypeManager = $entity_type_manager;
    $this->currentUser = $current_user;
    $this->logger = $logger;
    $this->classResolver = $class_resolver;
    $this->negotiatorIds = $negotiator_ids;
  }

  /**
   * {@inheritdoc}
   */
  public function entityTypeCanBelongToWorkspaces(EntityTypeInterface $entity_type) {
    if (!in_array($entity_type->id(), $this->blacklist, TRUE)
      && $entity_type->entityClassImplements(EntityPublishedInterface::class)
      && $entity_type->isRevisionable()) {
      return TRUE;
    }
    $this->blacklist[] = $entity_type->id();
    return FALSE;
  }

  /**
   * {@inheritdoc}
   */
  public function getSupportedEntityTypes() {
    $entity_types = [];
    foreach ($this->entityTypeManager->getDefinitions() as $entity_type_id => $entity_type) {
      if ($this->entityTypeCanBelongToWorkspaces($entity_type)) {
        $entity_types[$entity_type_id] = $entity_type;
      }
    }
    return $entity_types;
  }

  /**
   * {@inheritdoc}
   */
  public function getActiveWorkspace() {
    $request = $this->requestStack->getCurrentRequest();
    foreach ($this->negotiatorIds as $negotiator_id) {
      $negotiator = $this->classResolver->getInstanceFromDefinition($negotiator_id);
      if ($negotiator->applies($request)) {
        if ($active_workspace = $negotiator->getActiveWorkspace($request)) {
          break;
        }
      }
    }

    // The default workspace negotiator always returns a valid workspace.
    return $active_workspace;
  }

  /**
   * {@inheritdoc}
   */
  public function setActiveWorkspace(WorkspaceInterface $workspace) {
    // If the current user doesn't have access to view the workspace, they
    // shouldn't be allowed to switch to it.
    if (!$workspace->access('view') && !$workspace->isDefaultWorkspace()) {
      $this->logger->error('Denied access to view workspace {workspace}', ['workspace' => $workspace->label()]);
      throw new WorkspaceAccessException('The user does not have permission to view that workspace.');
    }

    // Set the workspace on the proper negotiator.
    $request = $this->requestStack->getCurrentRequest();
    foreach ($this->negotiatorIds as $negotiator_id) {
      $negotiator = $this->classResolver->getInstanceFromDefinition($negotiator_id);
      if ($negotiator->applies($request)) {
        $negotiator->setActiveWorkspace($workspace);
        break;
      }
    }

    $supported_entity_types = $this->getSupportedEntityTypes();
    foreach ($supported_entity_types as $supported_entity_type) {
      $this->entityTypeManager->getStorage($supported_entity_type->id())->resetCache();
    }

    return $this;
  }

}
